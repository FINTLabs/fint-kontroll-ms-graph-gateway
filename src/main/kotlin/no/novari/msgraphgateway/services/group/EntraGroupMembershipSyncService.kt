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
import no.novari.msgraphgateway.repository.device.DeviceMembershipEntity
import no.novari.msgraphgateway.repository.device.DeviceMembershipEntityRepository
import no.novari.msgraphgateway.repository.device.DeviceMembershipId
import no.novari.msgraphgateway.repository.user.UserMembershipEntity
import no.novari.msgraphgateway.repository.user.UserMembershipEntityRepository
import no.novari.msgraphgateway.repository.user.UserMembershipId
import org.springframework.stereotype.Service
import java.time.OffsetDateTime
import java.util.UUID

@Service
class EntraGroupMembershipSyncService(
    private val userMembershipRepository: UserMembershipEntityRepository,
    private val deviceMembershipRepository: DeviceMembershipEntityRepository,
    private val userMembershipProducer: EntraUserMembershipProducer,
    private val deviceMembershipProducer: EntraDeviceMembershipProducer,
    private val membershipRestoreService: EntraMembershipRestoreService,
) {
    fun replaceGroupMemberships(
        groupId: UUID,
        members: Collection<DirectoryObject>,
    ) {
        val now = OffsetDateTime.now()
        val userIds = members.filterIsInstance<User>().mapNotNull { it.id.toUuidOrNull() }
        val deviceIds = members.filterIsInstance<Device>().mapNotNull { it.id.toUuidOrNull() }

        val userRepairErrors = publishUserBaselineChanges(groupId, userIds)
        val deviceRepairErrors = publishDeviceBaselineChanges(groupId, deviceIds)
        userMembershipRepository.replaceGroupMemberships(groupId, userIds, now)
        deviceMembershipRepository.replaceGroupMemberships(groupId, deviceIds, now)
        userMembershipRepository.saveAll(userRepairErrors)
        deviceMembershipRepository.saveAll(deviceRepairErrors)
    }

    fun processDeltaPage(groups: Collection<Group>?): Int {
        if (groups.isNullOrEmpty()) return 0

        val now = OffsetDateTime.now()
        val userChanges = mutableListOf<UserMembershipEntity>()
        val deviceChanges = mutableListOf<DeviceMembershipEntity>()

        groups.forEach { group ->
            val groupId = group.id.toUuidOrNull() ?: return@forEach
            readMemberChanges(group).forEach { change ->
                when (change.type) {
                    MEMBER_TYPE_USER -> {
                        userChanges +=
                            UserMembershipEntity(
                                id = UserMembershipId(change.memberId, groupId),
                                status = change.status,
                                createdAt = now,
                                lastUpdatedAt = now,
                            )
                    }

                    MEMBER_TYPE_DEVICE -> {
                        deviceChanges +=
                            DeviceMembershipEntity(
                                id = DeviceMembershipId(change.memberId, groupId),
                                status = change.status,
                                createdAt = now,
                                lastUpdatedAt = now,
                            )
                    }
                }
            }
        }

        return processUserChanges(userChanges) + processDeviceChanges(deviceChanges)
    }

    private fun publishUserBaselineChanges(
        groupId: UUID,
        currentUserIds: Collection<UUID>,
    ): List<UserMembershipEntity> {
        val currentIds = currentUserIds.toSet()
        val existing = userMembershipRepository.findAllByGroupRef(groupId)

        currentIds
            .filter { userId -> existing[UserMembershipId(userId, groupId)]?.status != EntraStatus.ADDED }
            .forEach { userId -> publishUserMembership(userMembership(userId, groupId, EntraStatus.ADDED)) }
        return existing.values
            .filter { it.status == EntraStatus.ADDED && it.id.userRef !in currentIds }
            .mapNotNull { membership ->
                restoreUserMembership(membership)
            }
    }

    private fun publishDeviceBaselineChanges(
        groupId: UUID,
        currentDeviceIds: Collection<UUID>,
    ): List<DeviceMembershipEntity> {
        val currentIds = currentDeviceIds.toSet()
        val existing = deviceMembershipRepository.findAllByGroupRef(groupId)

        currentIds
            .filter { deviceId -> existing[DeviceMembershipId(deviceId, groupId)]?.status != EntraStatus.ADDED }
            .forEach { deviceId -> publishDeviceMembership(deviceMembership(deviceId, groupId, EntraStatus.ADDED)) }
        return existing.values
            .filter { it.status == EntraStatus.ADDED && it.id.deviceRef !in currentIds }
            .mapNotNull { membership ->
                restoreDeviceMembership(membership)
            }
    }

    private fun processUserChanges(changes: Collection<UserMembershipEntity>): Int {
        val distinctChanges = changes.associateBy { it.id }.values
        val existing = userMembershipRepository.findAllByIds(distinctChanges.map { it.id })
        val changesToSave = mutableListOf<UserMembershipEntity>()
        var published = 0

        distinctChanges.forEach { change ->
            val stored = existing[change.id]
            if (change.status == EntraStatus.REMOVED && stored?.status == EntraStatus.ADDED) {
                restoreUserMembership(stored)?.let(changesToSave::add)
                published++
            } else if (stored?.status != change.status) {
                publishUserMembership(change)
                changesToSave += change
                published++
            }
        }

        userMembershipRepository.saveAll(changesToSave)
        return published
    }

    private fun processDeviceChanges(changes: Collection<DeviceMembershipEntity>): Int {
        val distinctChanges = changes.associateBy { it.id }.values
        val existing = deviceMembershipRepository.findAllByIds(distinctChanges.map { it.id })
        val changesToSave = mutableListOf<DeviceMembershipEntity>()
        var published = 0

        distinctChanges.forEach { change ->
            val stored = existing[change.id]
            if (change.status == EntraStatus.REMOVED && stored?.status == EntraStatus.ADDED) {
                restoreDeviceMembership(stored)?.let(changesToSave::add)
                published++
            } else if (stored?.status != change.status) {
                publishDeviceMembership(change)
                changesToSave += change
                published++
            }
        }

        deviceMembershipRepository.saveAll(changesToSave)
        return published
    }

    private fun restoreUserMembership(membership: UserMembershipEntity): UserMembershipEntity? {
        val result = membershipRestoreService.restoreUserMembership(membership.id.groupRef, membership.id.userRef)
        val status = if (result == MembershipRestoreResult.RESTORED) EntraStatus.ADDED else EntraStatus.ERROR
        val resultMembership = userMembership(membership.id.userRef, membership.id.groupRef, status)
        publishUserMembership(resultMembership)
        return resultMembership.takeIf { it.status == EntraStatus.ERROR }
    }

    private fun restoreDeviceMembership(membership: DeviceMembershipEntity): DeviceMembershipEntity? {
        val result = membershipRestoreService.restoreDeviceMembership(membership.id.groupRef, membership.id.deviceRef)
        val status = if (result == MembershipRestoreResult.RESTORED) EntraStatus.ADDED else EntraStatus.ERROR
        val resultMembership = deviceMembership(membership.id.deviceRef, membership.id.groupRef, status)
        publishDeviceMembership(resultMembership)
        return resultMembership.takeIf { it.status == EntraStatus.ERROR }
    }

    private fun publishUserMembership(membership: UserMembershipEntity) {
        val userId = membership.id.userRef.toString()
        val groupId = membership.id.groupRef.toString()
        userMembershipProducer.publish(
            membershipKey(groupId, userId),
            EntraUserMembershipDto(membership.status, groupId, userId),
        )
    }

    private fun publishDeviceMembership(membership: DeviceMembershipEntity) {
        val deviceId = membership.id.deviceRef.toString()
        val groupId = membership.id.groupRef.toString()
        deviceMembershipProducer.publish(
            membershipKey(groupId, deviceId),
            EntraDeviceMembershipDto(membership.status, groupId, deviceId),
        )
    }

    private fun userMembership(
        userId: UUID,
        groupId: UUID,
        status: EntraStatus,
    ): UserMembershipEntity =
        UserMembershipEntity(
            id = UserMembershipId(userId, groupId),
            status = status,
            createdAt = OffsetDateTime.now(),
            lastUpdatedAt = OffsetDateTime.now(),
        )

    private fun deviceMembership(
        deviceId: UUID,
        groupId: UUID,
        status: EntraStatus,
    ): DeviceMembershipEntity =
        DeviceMembershipEntity(
            id = DeviceMembershipId(deviceId, groupId),
            status = status,
            createdAt = OffsetDateTime.now(),
            lastUpdatedAt = OffsetDateTime.now(),
        )

    private fun membershipKey(
        groupId: String,
        memberId: String,
    ): String = "$groupId:$memberId"

    private fun readMemberChanges(group: Group): List<MemberChange> {
        val membersDelta = group.additionalData[MEMBERS_DELTA] ?: return emptyList()
        val nodes = (membersDelta as? UntypedArray)?.value ?: return emptyList()

        return nodes.mapNotNull { node ->
            val values = (node as? UntypedObject)?.value ?: return@mapNotNull null
            val memberId = (values["id"] as? UntypedString)?.value.toUuidOrNull() ?: return@mapNotNull null
            val type = (values["@odata.type"] as? UntypedString)?.value ?: return@mapNotNull null
            val status = if (values.containsKey("@removed")) EntraStatus.REMOVED else EntraStatus.ADDED
            MemberChange(memberId, type, status)
        }
    }

    private fun String?.toUuidOrNull(): UUID? =
        this?.takeIf { it.isNotBlank() }?.let { runCatching { UUID.fromString(it) }.getOrNull() }

    private data class MemberChange(
        val memberId: UUID,
        val type: String,
        val status: EntraStatus,
    )

    companion object {
        private const val MEMBERS_DELTA = "members@delta"
        private const val MEMBER_TYPE_USER = "#microsoft.graph.user"
        private const val MEMBER_TYPE_DEVICE = "#microsoft.graph.device"
    }
}
