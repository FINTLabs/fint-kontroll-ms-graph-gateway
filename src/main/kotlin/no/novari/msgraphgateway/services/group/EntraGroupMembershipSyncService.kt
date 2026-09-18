package no.novari.msgraphgateway.services.group

import com.microsoft.graph.models.Device
import com.microsoft.graph.models.DirectoryObject
import com.microsoft.graph.models.Group
import com.microsoft.graph.models.User
import com.microsoft.kiota.serialization.UntypedArray
import com.microsoft.kiota.serialization.UntypedObject
import com.microsoft.kiota.serialization.UntypedString
import no.novari.msgraphgateway.dto.EntraDeviceMembershipDto
import no.novari.msgraphgateway.dto.EntraUserMembershipDto
import no.novari.msgraphgateway.entra.EntraStatus
import no.novari.msgraphgateway.kafka.membership.EntraDeviceMembershipProducer
import no.novari.msgraphgateway.kafka.membership.EntraUserMembershipProducer
import no.novari.msgraphgateway.repository.device.DeviceMembershipId
import no.novari.msgraphgateway.repository.membership.GroupMembershipStateRepository
import no.novari.msgraphgateway.repository.user.UserMembershipId
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

@Service
class EntraGroupMembershipSyncService(
    private val membershipStateRepository: GroupMembershipStateRepository,
    private val userMembershipProducer: EntraUserMembershipProducer,
    private val deviceMembershipProducer: EntraDeviceMembershipProducer,
) {
    fun stageGroupMemberships(
        runId: UUID,
        groupId: UUID,
        members: Collection<DirectoryObject>,
    ) {
        val changes =
            members.mapNotNull { member ->
                toMemberChange(
                    memberId = member.id,
                    graphType = member.odataType ?: member.graphTypeFromRuntimeClass(),
                    status = EntraStatus.ADDED,
                    groupId = groupId,
                )
            }
        applySnapshotChanges(runId, groupId, changes)
    }

    fun processDeltaPage(
        groups: Collection<Group>?,
        snapshotRunId: UUID? = null,
    ): Int {
        if (groups.isNullOrEmpty()) return 0

        var processed = 0
        groups.forEach { group ->
            val groupId = group.id.toUuidOrNull() ?: return@forEach
            if (group.additionalData.containsKey(REMOVED)) {
                processed += processRemovedGroup(groupId, snapshotRunId)
                return@forEach
            }

            val changes = readMemberChanges(group)
            if (snapshotRunId == null) {
                applyObservedChanges(groupId, changes)
            } else {
                applySnapshotChanges(snapshotRunId, groupId, changes)
            }
            processed += changes.size
        }
        return processed
    }

    fun completeSnapshot(
        runId: UUID,
        initialBootstrap: Boolean,
        republishAll: Boolean,
    ): MembershipSnapshotResult {
        val added = AtomicInteger(0)
        val removed = AtomicInteger(0)
        val publishAllAdditions = initialBootstrap || republishAll

        membershipStateRepository.forEachSnapshotUserAddition(runId, publishAllAdditions) { membership ->
            publishUserMembership(membership, EntraStatus.ADDED)
            added.incrementAndGet()
        }
        membershipStateRepository.forEachSnapshotDeviceAddition(runId, publishAllAdditions) { membership ->
            publishDeviceMembership(membership, EntraStatus.ADDED)
            added.incrementAndGet()
        }

        if (!initialBootstrap) {
            membershipStateRepository.forEachMissingUser(runId) { membership ->
                publishUserMembership(membership, EntraStatus.REMOVED)
                removed.incrementAndGet()
            }
            membershipStateRepository.forEachMissingDevice(runId) { membership ->
                publishDeviceMembership(membership, EntraStatus.REMOVED)
                removed.incrementAndGet()
            }
        }

        membershipStateRepository.completeSnapshot(runId)
        return MembershipSnapshotResult(added.get(), removed.get())
    }

    fun discardSnapshot(runId: UUID) {
        membershipStateRepository.discardSnapshot(runId)
    }

    private fun applySnapshotChanges(
        runId: UUID,
        groupId: UUID,
        changes: Collection<MemberChange>,
    ) {
        val userChanges = changes.filter { it.type == MemberType.USER }.associateBy { it.memberId }.values
        val deviceChanges = changes.filter { it.type == MemberType.DEVICE }.associateBy { it.memberId }.values

        membershipStateRepository.markUsersSeen(
            runId,
            userChanges
                .filter { it.status == EntraStatus.ADDED }
                .map { UserMembershipId(it.memberId, groupId) },
        )
        membershipStateRepository.unmarkUsersSeen(
            runId,
            userChanges
                .filter { it.status == EntraStatus.REMOVED }
                .map { UserMembershipId(it.memberId, groupId) },
        )
        membershipStateRepository.markDevicesSeen(
            runId,
            deviceChanges
                .filter { it.status == EntraStatus.ADDED }
                .map { DeviceMembershipId(it.memberId, groupId) },
        )
        membershipStateRepository.unmarkDevicesSeen(
            runId,
            deviceChanges
                .filter { it.status == EntraStatus.REMOVED }
                .map { DeviceMembershipId(it.memberId, groupId) },
        )
    }

    private fun applyObservedChanges(
        groupId: UUID,
        changes: Collection<MemberChange>,
    ) {
        val userChanges = changes.filter { it.type == MemberType.USER }.associateBy { it.memberId }.values
        val deviceChanges = changes.filter { it.type == MemberType.DEVICE }.associateBy { it.memberId }.values

        val addedUsers =
            userChanges
                .filter { it.status == EntraStatus.ADDED }
                .map { UserMembershipId(it.memberId, groupId) }
        val removedUsers =
            userChanges
                .filter { it.status == EntraStatus.REMOVED }
                .map { UserMembershipId(it.memberId, groupId) }
        val addedDevices =
            deviceChanges
                .filter { it.status == EntraStatus.ADDED }
                .map { DeviceMembershipId(it.memberId, groupId) }
        val removedDevices =
            deviceChanges
                .filter { it.status == EntraStatus.REMOVED }
                .map { DeviceMembershipId(it.memberId, groupId) }

        membershipStateRepository.addObservedUsers(addedUsers)
        membershipStateRepository.removeObservedUsers(removedUsers)
        membershipStateRepository.addObservedDevices(addedDevices)
        membershipStateRepository.removeObservedDevices(removedDevices)

        addedUsers.forEach { publishUserMembership(it, EntraStatus.ADDED) }
        removedUsers.forEach { publishUserMembership(it, EntraStatus.REMOVED) }
        addedDevices.forEach { publishDeviceMembership(it, EntraStatus.ADDED) }
        removedDevices.forEach { publishDeviceMembership(it, EntraStatus.REMOVED) }
    }

    private fun processRemovedGroup(
        groupId: UUID,
        snapshotRunId: UUID?,
    ): Int {
        if (snapshotRunId != null) {
            membershipStateRepository.unmarkGroupSeen(snapshotRunId, groupId)
            return 0
        }

        val users = membershipStateRepository.findObservedUsersByGroup(groupId)
        val devices = membershipStateRepository.findObservedDevicesByGroup(groupId)
        membershipStateRepository.removeObservedUsers(users)
        membershipStateRepository.removeObservedDevices(devices)
        users.forEach { publishUserMembership(it, EntraStatus.REMOVED) }
        devices.forEach { publishDeviceMembership(it, EntraStatus.REMOVED) }
        return users.size + devices.size
    }

    private fun publishUserMembership(
        membership: UserMembershipId,
        status: EntraStatus,
    ) {
        val userId = membership.userRef.toString()
        val groupId = membership.groupRef.toString()
        userMembershipProducer.publish(
            membershipKey(groupId, userId),
            EntraUserMembershipDto(status, groupId, userId),
        )
    }

    private fun publishDeviceMembership(
        membership: DeviceMembershipId,
        status: EntraStatus,
    ) {
        val deviceId = membership.deviceRef.toString()
        val groupId = membership.groupRef.toString()
        deviceMembershipProducer.publish(
            membershipKey(groupId, deviceId),
            EntraDeviceMembershipDto(status, groupId, deviceId),
        )
    }

    private fun membershipKey(
        groupId: String,
        memberId: String,
    ): String = "$groupId:$memberId"

    private fun readMemberChanges(group: Group): List<MemberChange> {
        val membersDelta = group.additionalData[MEMBERS_DELTA] ?: return emptyList()
        val nodes = (membersDelta as? UntypedArray)?.value ?: return emptyList()

        return nodes.mapNotNull { node ->
            val values = (node as? UntypedObject)?.value ?: return@mapNotNull null
            val status = if (values.containsKey(REMOVED)) EntraStatus.REMOVED else EntraStatus.ADDED
            toMemberChange(
                memberId = (values["id"] as? UntypedString)?.value,
                graphType = (values[ODATA_TYPE] as? UntypedString)?.value,
                status = status,
            )
        }
    }

    private fun toMemberChange(
        memberId: String?,
        graphType: String?,
        status: EntraStatus,
        groupId: UUID? = null,
    ): MemberChange? {
        val id = memberId.toUuidOrNull() ?: return null
        val type = MemberType.fromGraphType(graphType)
        if (type == MemberType.UNKNOWN) {
            log.warn(
                "Ignoring unsupported Graph group member type {} for memberId {}{}",
                graphType ?: "<missing>",
                id,
                groupId?.let { " in groupId $it" }.orEmpty(),
            )
            return null
        }
        return MemberChange(id, type, status)
    }

    private fun DirectoryObject.graphTypeFromRuntimeClass(): String? =
        when (this) {
            is User -> MEMBER_TYPE_USER
            is Device -> MEMBER_TYPE_DEVICE
            else -> null
        }

    private fun String?.toUuidOrNull(): UUID? =
        this?.takeIf { it.isNotBlank() }?.let { runCatching { UUID.fromString(it) }.getOrNull() }

    private data class MemberChange(
        val memberId: UUID,
        val type: MemberType,
        val status: EntraStatus,
    )

    private enum class MemberType {
        USER,
        DEVICE,
        UNKNOWN,
        ;

        companion object {
            fun fromGraphType(graphType: String?): MemberType =
                when (graphType?.lowercase()) {
                    MEMBER_TYPE_USER -> USER
                    MEMBER_TYPE_DEVICE -> DEVICE
                    else -> UNKNOWN
                }
        }
    }

    data class MembershipSnapshotResult(
        val added: Int,
        val removed: Int,
    )

    companion object {
        private const val MEMBERS_DELTA = "members@delta"
        private const val REMOVED = "@removed"
        private const val ODATA_TYPE = "@odata.type"
        private const val MEMBER_TYPE_USER = "#microsoft.graph.user"
        private const val MEMBER_TYPE_DEVICE = "#microsoft.graph.device"
        private val log = LoggerFactory.getLogger(EntraGroupMembershipSyncService::class.java)
    }
}
