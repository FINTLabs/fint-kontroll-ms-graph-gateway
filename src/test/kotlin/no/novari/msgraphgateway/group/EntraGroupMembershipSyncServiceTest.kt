package no.novari.msgraphgateway.group

import com.microsoft.graph.models.DirectoryObject
import com.microsoft.graph.models.Group
import com.microsoft.kiota.serialization.UntypedArray
import com.microsoft.kiota.serialization.UntypedNode
import com.microsoft.kiota.serialization.UntypedObject
import com.microsoft.kiota.serialization.UntypedString
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.novari.msgraphgateway.entra.EntraStatus
import no.novari.msgraphgateway.kafka.membership.EntraDeviceMembershipProducer
import no.novari.msgraphgateway.kafka.membership.EntraUserMembershipProducer
import no.novari.msgraphgateway.repository.device.DeviceMembershipId
import no.novari.msgraphgateway.repository.membership.GroupMembershipStateRepository
import no.novari.msgraphgateway.repository.user.UserMembershipId
import no.novari.msgraphgateway.services.group.EntraGroupMembershipSyncService
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class EntraGroupMembershipSyncServiceTest {
    private val stateRepository = mockk<GroupMembershipStateRepository>(relaxed = true)
    private val userProducer = mockk<EntraUserMembershipProducer>(relaxed = true)
    private val deviceProducer = mockk<EntraDeviceMembershipProducer>(relaxed = true)
    private val service = EntraGroupMembershipSyncService(stateRepository, userProducer, deviceProducer)

    @Test
    fun `stageGroupMemberships separates users and devices without publishing`() {
        val runId = UUID.randomUUID()
        val groupId = UUID.randomUUID()
        val userId = UUID.randomUUID()
        val deviceId = UUID.randomUUID()

        service.stageGroupMemberships(
            runId,
            groupId,
            listOf(
                directoryObject(userId, "#microsoft.graph.user"),
                directoryObject(deviceId, "#microsoft.graph.device"),
            ),
        )

        verify(exactly = 1) {
            stateRepository.markUsersSeen(runId, listOf(UserMembershipId(userId, groupId)))
        }
        verify(exactly = 1) {
            stateRepository.markDevicesSeen(runId, listOf(DeviceMembershipId(deviceId, groupId)))
        }
        verify(exactly = 0) { userProducer.publish(any(), any()) }
        verify(exactly = 0) { deviceProducer.publish(any(), any()) }
    }

    @Test
    fun `unsupported member type is ignored`() {
        val runId = UUID.randomUUID()
        val groupId = UUID.randomUUID()

        service.stageGroupMemberships(
            runId,
            groupId,
            listOf(directoryObject(UUID.randomUUID(), "#microsoft.graph.servicePrincipal")),
        )

        verify(exactly = 1) { stateRepository.markUsersSeen(runId, emptyList()) }
        verify(exactly = 1) { stateRepository.markDevicesSeen(runId, emptyList()) }
        verify(exactly = 0) { userProducer.publish(any(), any()) }
        verify(exactly = 0) { deviceProducer.publish(any(), any()) }
    }

    @Test
    fun `normal delta stores and publishes observed membership changes`() {
        val groupId = UUID.randomUUID()
        val userId = UUID.randomUUID()
        val deviceId = UUID.randomUUID()
        val group =
            groupWithChanges(
                groupId,
                memberNode(userId, "#microsoft.graph.user"),
                memberNode(deviceId, "#microsoft.graph.device", removed = true),
            )

        val processed = service.processDeltaPage(listOf(group))

        assertEquals(2, processed)
        verify { stateRepository.addObservedUsers(listOf(UserMembershipId(userId, groupId))) }
        verify { stateRepository.removeObservedDevices(listOf(DeviceMembershipId(deviceId, groupId))) }
        verify {
            userProducer.publish("$groupId:$userId", match { it.code == EntraStatus.ADDED })
        }
        verify {
            deviceProducer.publish("$groupId:$deviceId", match { it.code == EntraStatus.REMOVED })
        }
    }

    @Test
    fun `delta catch-up changes staging without publishing`() {
        val runId = UUID.randomUUID()
        val groupId = UUID.randomUUID()
        val userId = UUID.randomUUID()
        val group = groupWithChanges(groupId, memberNode(userId, "#microsoft.graph.user", removed = true))

        service.processDeltaPage(listOf(group), runId)

        verify { stateRepository.unmarkUsersSeen(runId, listOf(UserMembershipId(userId, groupId))) }
        verify(exactly = 0) { userProducer.publish(any(), any()) }
    }

    @Test
    fun `initial bootstrap republishes found memberships but not missing memberships`() {
        val runId = UUID.randomUUID()
        val groupId = UUID.randomUUID()
        val userId = UUID.randomUUID()
        val deviceId = UUID.randomUUID()
        val userMembership = UserMembershipId(userId, groupId)
        val deviceMembership = DeviceMembershipId(deviceId, groupId)

        every { stateRepository.forEachSnapshotUserAddition(runId, true, any()) } answers {
            arg<(UserMembershipId) -> Unit>(2).invoke(userMembership)
        }
        every { stateRepository.forEachSnapshotDeviceAddition(runId, true, any()) } answers {
            arg<(DeviceMembershipId) -> Unit>(2).invoke(deviceMembership)
        }

        val result = service.completeSnapshot(runId, initialBootstrap = true, republishAll = false)

        assertEquals(EntraGroupMembershipSyncService.MembershipSnapshotResult(2, 0), result)
        verify { userProducer.publish("$groupId:$userId", match { it.code == EntraStatus.ADDED }) }
        verify { deviceProducer.publish("$groupId:$deviceId", match { it.code == EntraStatus.ADDED }) }
        verify(exactly = 0) { stateRepository.forEachMissingUser(any(), any()) }
        verify(exactly = 0) { stateRepository.forEachMissingDevice(any(), any()) }
        verify { stateRepository.completeSnapshot(runId) }
    }

    @Test
    fun `completed resnapshot publishes memberships missing from Graph as removed`() {
        val runId = UUID.randomUUID()
        val groupId = UUID.randomUUID()
        val userId = UUID.randomUUID()
        val missing = UserMembershipId(userId, groupId)

        every { stateRepository.forEachMissingUser(runId, any()) } answers {
            arg<(UserMembershipId) -> Unit>(1).invoke(missing)
        }

        val result = service.completeSnapshot(runId, initialBootstrap = false, republishAll = false)

        assertEquals(EntraGroupMembershipSyncService.MembershipSnapshotResult(0, 1), result)
        verify { userProducer.publish("$groupId:$userId", match { it.code == EntraStatus.REMOVED }) }
        verify { stateRepository.completeSnapshot(runId) }
    }

    private fun groupWithChanges(
        groupId: UUID,
        vararg changes: UntypedObject,
    ): Group =
        Group().apply {
            id = groupId.toString()
            additionalData["members@delta"] = UntypedArray(changes.toList())
        }

    private fun memberNode(
        id: UUID,
        type: String,
        removed: Boolean = false,
    ): UntypedObject {
        val values: MutableMap<String, UntypedNode> =
            mutableMapOf(
                "id" to UntypedString(id.toString()),
                "@odata.type" to UntypedString(type),
            )
        if (removed) values["@removed"] = UntypedObject(emptyMap())
        return UntypedObject(values)
    }

    private fun directoryObject(
        id: UUID,
        graphType: String,
    ): DirectoryObject =
        DirectoryObject().apply {
            this.id = id.toString()
            odataType = graphType
        }
}
