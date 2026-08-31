package no.novari.msgraphgateway.group

import com.microsoft.graph.models.Device
import com.microsoft.graph.models.Group
import com.microsoft.graph.models.User
import com.microsoft.kiota.serialization.UntypedArray
import com.microsoft.kiota.serialization.UntypedNode
import com.microsoft.kiota.serialization.UntypedObject
import com.microsoft.kiota.serialization.UntypedString
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import no.novari.msgraphgateway.entra.EntraStatus
import no.novari.msgraphgateway.kafka.membership.EntraDeviceMembershipProducer
import no.novari.msgraphgateway.kafka.membership.EntraUserMembershipProducer
import no.novari.msgraphgateway.repository.device.DeviceMembershipEntity
import no.novari.msgraphgateway.repository.device.DeviceMembershipEntityRepository
import no.novari.msgraphgateway.repository.user.UserMembershipEntity
import no.novari.msgraphgateway.repository.user.UserMembershipEntityRepository
import no.novari.msgraphgateway.repository.user.UserMembershipId
import no.novari.msgraphgateway.services.group.EntraGroupMembershipSyncService
import no.novari.msgraphgateway.services.group.EntraMembershipRestoreService
import no.novari.msgraphgateway.services.group.MembershipRestoreResult
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

class EntraGroupMembershipSyncServiceTest {
    private val userRepository = mockk<UserMembershipEntityRepository>(relaxed = true)
    private val deviceRepository = mockk<DeviceMembershipEntityRepository>(relaxed = true)
    private val userProducer = mockk<EntraUserMembershipProducer>(relaxed = true)
    private val deviceProducer = mockk<EntraDeviceMembershipProducer>(relaxed = true)
    private val restoreService = mockk<EntraMembershipRestoreService>(relaxed = true)
    private val service =
        EntraGroupMembershipSyncService(
            userRepository,
            deviceRepository,
            userProducer,
            deviceProducer,
            restoreService,
        )

    @Test
    fun `replaceGroupMemberships separates users and devices`() {
        val groupId = UUID.randomUUID()
        val userId = UUID.randomUUID()
        val deviceId = UUID.randomUUID()
        val users = slot<Collection<UUID>>()
        val devices = slot<Collection<UUID>>()

        every { userRepository.replaceGroupMemberships(groupId, capture(users), any()) } returns Unit
        every { deviceRepository.replaceGroupMemberships(groupId, capture(devices), any()) } returns Unit

        service.replaceGroupMemberships(
            groupId,
            listOf(
                User().apply { id = userId.toString() },
                Device().apply { id = deviceId.toString() },
            ),
        )

        assertEquals(listOf(userId), users.captured)
        assertEquals(listOf(deviceId), devices.captured)
        verify(exactly = 1) {
            userProducer.publish(
                "$groupId:$userId",
                match { it.code == EntraStatus.ADDED && it.entraGroupRef == groupId.toString() },
            )
        }
        verify(exactly = 1) {
            deviceProducer.publish(
                "$groupId:$deviceId",
                match { it.code == EntraStatus.ADDED && it.entraGroupRef == groupId.toString() },
            )
        }
    }

    @Test
    fun `processDeltaPage applies added user and removed device`() {
        val groupId = UUID.randomUUID()
        val userId = UUID.randomUUID()
        val deviceId = UUID.randomUUID()
        val savedUsers = slot<Collection<UserMembershipEntity>>()
        val savedDevices = slot<Collection<DeviceMembershipEntity>>()
        val group =
            Group().apply {
                id = groupId.toString()
                additionalData["members@delta"] =
                    UntypedArray(
                        listOf(
                            memberNode(userId, "#microsoft.graph.user"),
                            memberNode(deviceId, "#microsoft.graph.device", removed = true),
                        ),
                    )
            }

        every { userRepository.saveAll(capture(savedUsers)) } returns Unit
        every { deviceRepository.saveAll(capture(savedDevices)) } returns Unit

        service.processDeltaPage(listOf(group))

        assertEquals(EntraStatus.ADDED, savedUsers.captured.single().status)
        assertEquals(
            userId,
            savedUsers.captured
                .single()
                .id.userRef,
        )
        assertEquals(
            groupId,
            savedUsers.captured
                .single()
                .id.groupRef,
        )
        assertEquals(EntraStatus.REMOVED, savedDevices.captured.single().status)
        assertEquals(
            deviceId,
            savedDevices.captured
                .single()
                .id.deviceRef,
        )
        assertEquals(
            groupId,
            savedDevices.captured
                .single()
                .id.groupRef,
        )
        verify(exactly = 1) { userRepository.saveAll(any()) }
        verify(exactly = 1) { deviceRepository.saveAll(any()) }
        verify(exactly = 1) {
            userProducer.publish("$groupId:$userId", match { it.code == EntraStatus.ADDED })
        }
        verify(exactly = 1) {
            deviceProducer.publish("$groupId:$deviceId", match { it.code == EntraStatus.REMOVED })
        }
    }

    @Test
    fun `processDeltaPage skips Kafka when status is already stored`() {
        val groupId = UUID.randomUUID()
        val userId = UUID.randomUUID()
        val existing =
            UserMembershipEntity(
                id = UserMembershipId(userId, groupId),
                status = EntraStatus.ADDED,
                createdAt = OffsetDateTime.now(),
                lastUpdatedAt = OffsetDateTime.now(),
            )
        val group =
            Group().apply {
                id = groupId.toString()
                additionalData["members@delta"] =
                    UntypedArray(listOf(memberNode(userId, "#microsoft.graph.user")))
            }

        every { userRepository.findAllByIds(any()) } returns mapOf(existing.id to existing)

        service.processDeltaPage(listOf(group))

        verify(exactly = 0) { userProducer.publish(any(), any()) }
    }

    @Test
    fun `removed user is restored when desired status is added`() {
        val groupId = UUID.randomUUID()
        val userId = UUID.randomUUID()
        val existing = userMembership(userId, groupId, EntraStatus.ADDED)
        val group = groupWithMemberChange(groupId, userId, "#microsoft.graph.user", removed = true)

        every { userRepository.findAllByIds(any()) } returns mapOf(existing.id to existing)
        every { restoreService.restoreUserMembership(groupId, userId) } returns MembershipRestoreResult.RESTORED

        service.processDeltaPage(listOf(group))

        verify(exactly = 1) { restoreService.restoreUserMembership(groupId, userId) }
        verify(exactly = 1) {
            userProducer.publish("$groupId:$userId", match { it.code == EntraStatus.ADDED })
        }
        verify(exactly = 0) {
            userProducer.publish(any(), match { it.code == EntraStatus.REMOVED })
        }
        verify(exactly = 0) { userRepository.saveAll(match { it.isNotEmpty() }) }
    }

    @Test
    fun `removed user publishes error when membership cannot be restored`() {
        val groupId = UUID.randomUUID()
        val userId = UUID.randomUUID()
        val existing = userMembership(userId, groupId, EntraStatus.ADDED)
        val group = groupWithMemberChange(groupId, userId, "#microsoft.graph.user", removed = true)

        every { userRepository.findAllByIds(any()) } returns mapOf(existing.id to existing)
        every { restoreService.restoreUserMembership(groupId, userId) } returns MembershipRestoreResult.NOT_POSSIBLE

        service.processDeltaPage(listOf(group))

        verify(exactly = 1) {
            userProducer.publish("$groupId:$userId", match { it.code == EntraStatus.ERROR })
        }
        verify(exactly = 0) {
            userProducer.publish(any(), match { it.code == EntraStatus.REMOVED })
        }
        verify(exactly = 1) {
            userRepository.saveAll(match { it.singleOrNull()?.status == EntraStatus.ERROR })
        }
    }

    private fun groupWithMemberChange(
        groupId: UUID,
        memberId: UUID,
        type: String,
        removed: Boolean,
    ): Group =
        Group().apply {
            id = groupId.toString()
            additionalData["members@delta"] = UntypedArray(listOf(memberNode(memberId, type, removed)))
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
}
