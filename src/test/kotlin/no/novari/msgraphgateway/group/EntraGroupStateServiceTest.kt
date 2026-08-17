package no.novari.msgraphgateway.group

import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import no.novari.msgraphgateway.entra.EntraStatus
import no.novari.msgraphgateway.entra.group.EntraGroup
import no.novari.msgraphgateway.kafka.group.GroupProducerService
import no.novari.msgraphgateway.repository.group.GroupRepository
import no.novari.msgraphgateway.services.Checksum
import no.novari.msgraphgateway.services.ChecksumService
import no.novari.msgraphgateway.services.group.EntraGroupStateService
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.UUID

class EntraGroupStateServiceTest {
    private val groupRepository = mockk<GroupRepository>()
    private val checksumService = mockk<ChecksumService>()
    private val groupProducerService = mockk<GroupProducerService>()

    private val service =
        EntraGroupStateService(
            groupRepository = groupRepository,
            checksumService = checksumService,
            groupProducerService = groupProducerService,
        )

    @Test
    fun `findObjectIdByResourceGroupId returns stored objectId`() {
        val objectId = UUID.randomUUID()

        every { groupRepository.findObjectIdByResourceGroupId(12345L) } returns objectId

        val result = service.findObjectIdByResourceGroupId("12345")

        assertEquals(objectId.toString(), result)

        verify(exactly = 1) {
            groupRepository.findObjectIdByResourceGroupId(12345L)
        }
    }

    @Test
    fun `findObjectIdByResourceGroupId returns null when resourceGroupId is invalid`() {
        val result = service.findObjectIdByResourceGroupId("not-a-number")

        assertNull(result)

        verify(exactly = 0) {
            groupRepository.findObjectIdByResourceGroupId(any())
        }
    }

    @Test
    fun `findResourceGroupIdByObjectId returns stored resourceGroupId`() {
        val objectId = UUID.randomUUID()

        every { groupRepository.findResourceGroupIdByObjectId(objectId) } returns 12345L

        val result = service.findResourceGroupIdByObjectId(objectId.toString())

        assertEquals(12345L, result)

        verify(exactly = 1) {
            groupRepository.findResourceGroupIdByObjectId(objectId)
        }
    }

    @Test
    fun `findResourceGroupIdByObjectId returns null when objectId is invalid`() {
        val result = service.findResourceGroupIdByObjectId("not-a-uuid")

        assertNull(result)

        verify(exactly = 0) {
            groupRepository.findResourceGroupIdByObjectId(any())
        }
    }

    @Test
    fun `isUnchanged returns false when objectId is invalid`() {
        val result =
            service.isUnchanged(
                EntraGroup(
                    objectId = "not-a-uuid",
                    displayName = "TestGroup",
                    resourceGroupID = 12345,
                ),
            )

        assertFalse(result)

        verify(exactly = 0) {
            groupRepository.findChecksumById(any())
        }

        verify(exactly = 0) {
            checksumService.checksum(any())
        }
    }

    @Test
    fun `isUnchanged returns false when stored checksum is missing`() {
        val objectId = UUID.randomUUID()

        every { groupRepository.findChecksumById(objectId) } returns null

        val result =
            service.isUnchanged(
                EntraGroup(
                    objectId = objectId.toString(),
                    displayName = "TestGroup",
                    resourceGroupID = 12345,
                ),
            )

        assertFalse(result)

        verify(exactly = 1) {
            groupRepository.findChecksumById(objectId)
        }

        verify(exactly = 0) {
            checksumService.checksum(any())
        }
    }

    @Test
    fun `isUnchanged returns true when checksum matches`() {
        val objectId = UUID.randomUUID()
        val checksum = Checksum("same-checksum".toByteArray())

        every { groupRepository.findChecksumById(objectId) } returns checksum
        every { checksumService.checksum(any()) } returns checksum

        val result =
            service.isUnchanged(
                EntraGroup(
                    objectId = objectId.toString(),
                    displayName = "TestGroup",
                    resourceGroupID = 12345,
                ),
            )

        assertTrue(result)

        verify(exactly = 1) {
            groupRepository.findChecksumById(objectId)
        }

        verify(exactly = 1) {
            checksumService.checksum(any())
        }
    }

    @Test
    fun `isUnchanged compares checksum without traceId and status`() {
        val objectId = UUID.randomUUID()
        val checksum = Checksum("same-checksum".toByteArray())
        val incomingGroup =
            EntraGroup(
                objectId = objectId.toString(),
                displayName = "TestGroup",
                resourceGroupID = 12345,
                traceId = "trace-123",
                status = EntraStatus.UPDATED,
            )
        val expectedChecksumInput = incomingGroup.copy(traceId = null, status = null)

        every { groupRepository.findChecksumById(objectId) } returns checksum
        every { checksumService.checksum(expectedChecksumInput) } returns checksum

        val result = service.isUnchanged(incomingGroup)

        assertTrue(result)

        verify(exactly = 1) {
            checksumService.checksum(expectedChecksumInput)
        }
    }

    @Test
    fun `isUnchanged returns false when checksum differs`() {
        val objectId = UUID.randomUUID()

        every { groupRepository.findChecksumById(objectId) } returns Checksum("stored".toByteArray())
        every { checksumService.checksum(any()) } returns Checksum("incoming".toByteArray())

        val result =
            service.isUnchanged(
                EntraGroup(
                    objectId = objectId.toString(),
                    displayName = "TestGroup",
                    resourceGroupID = 12345,
                ),
            )

        assertFalse(result)

        verify(exactly = 1) {
            groupRepository.findChecksumById(objectId)
        }

        verify(exactly = 1) {
            checksumService.checksum(any())
        }
    }

    @Test
    fun `storeAndPublishIfChanged stores and publishes when group has changed`() {
        val objectId = UUID.randomUUID()
        val checksum = Checksum("checksum".toByteArray())
        val entraGroup =
            EntraGroup(
                objectId = objectId.toString(),
                displayName = "TestGroup",
                resourceGroupID = 12345,
            )

        every { checksumService.checksum(entraGroup) } returns checksum
        every {
            groupRepository.batchUpsertReturningChanged(
                match {
                    it.size == 1 &&
                        it.first().objectId == objectId &&
                        it.first().resourceGroupId == 12345L &&
                        it.first().checksum == checksum
                },
            )
        } returns setOf(objectId)
        every { groupProducerService.publish(any()) } just Runs

        val result = service.storeAndPublishIfChanged(entraGroup)

        assertTrue(result)

        verify(exactly = 1) {
            groupRepository.batchUpsertReturningChanged(any())
        }

        verify(exactly = 1) {
            groupProducerService.publish(
                match {
                    it.objectId == entraGroup.objectId &&
                        it.displayName == entraGroup.displayName &&
                        it.resourceGroupID == entraGroup.resourceGroupID &&
                        it.traceId == null &&
                        it.status == EntraStatus.CREATED
                },
            )
        }
    }

    @Test
    fun `storeAndPublishIfChanged does not publish when group is unchanged`() {
        val objectId = UUID.randomUUID()
        val checksum = Checksum("checksum".toByteArray())
        val entraGroup =
            EntraGroup(
                objectId = objectId.toString(),
                displayName = "TestGroup",
                resourceGroupID = 12345,
            )

        every { checksumService.checksum(entraGroup) } returns checksum
        every { groupRepository.batchUpsertReturningChanged(any()) } returns emptySet()

        val result = service.storeAndPublishIfChanged(entraGroup)

        assertFalse(result)

        verify(exactly = 1) {
            groupRepository.batchUpsertReturningChanged(any())
        }

        verify(exactly = 0) {
            groupProducerService.publish(any())
        }
    }

    @Test
    fun `storeAndPublish stores and publishes regardless of changed state`() {
        val objectId = UUID.randomUUID()
        val checksum = Checksum("checksum".toByteArray())
        val entraGroup =
            EntraGroup(
                objectId = objectId.toString(),
                displayName = "TestGroup",
                resourceGroupID = 12345,
            )

        every { checksumService.checksum(entraGroup) } returns checksum
        every {
            groupRepository.batchUpsert(
                match {
                    it.size == 1 &&
                        it.first().objectId == objectId &&
                        it.first().resourceGroupId == 12345L &&
                        it.first().checksum == checksum
                },
            )
        } just Runs
        every { groupProducerService.publish(any(), EntraStatus.CREATED) } just Runs

        val result = service.storeAndPublish(entraGroup)

        assertTrue(result)

        verify(exactly = 1) {
            groupRepository.batchUpsert(any())
        }

        verify(exactly = 0) {
            groupRepository.batchUpsertReturningChanged(any())
        }

        verify(exactly = 1) {
            groupProducerService.publish(
                match {
                    it.objectId == entraGroup.objectId &&
                        it.displayName == entraGroup.displayName &&
                        it.resourceGroupID == entraGroup.resourceGroupID &&
                        it.traceId == null &&
                        it.status == EntraStatus.CREATED
                },
                EntraStatus.CREATED,
            )
        }
    }

    @Test
    fun `publish publishes without storing local state`() {
        val objectId = UUID.randomUUID()
        val entraGroup =
            EntraGroup(
                objectId = objectId.toString(),
                displayName = "TestGroup",
                resourceGroupID = 12345,
            )

        every { groupProducerService.publish(any(), EntraStatus.FAILED) } just Runs

        val result = service.publish(entraGroup, "trace-123", EntraStatus.FAILED)

        assertTrue(result)

        verify(exactly = 1) {
            groupProducerService.publish(
                match {
                    it.objectId == entraGroup.objectId &&
                        it.displayName == entraGroup.displayName &&
                        it.resourceGroupID == entraGroup.resourceGroupID &&
                        it.traceId == "trace-123" &&
                        it.status == EntraStatus.FAILED
                },
                EntraStatus.FAILED,
            )
        }

        verify(exactly = 0) {
            groupRepository.batchUpsert(any())
            groupRepository.batchUpsertReturningChanged(any())
            checksumService.checksum(any())
        }
    }

    @Test
    fun `storeAndPublishIfChanged does not store or publish when objectId is invalid`() {
        val entraGroup =
            EntraGroup(
                objectId = "not-a-uuid",
                displayName = "TestGroup",
                resourceGroupID = 12345,
            )

        val result = service.storeAndPublishIfChanged(entraGroup)

        assertFalse(result)

        verify(exactly = 0) {
            checksumService.checksum(any())
        }

        verify(exactly = 0) {
            groupRepository.batchUpsertReturningChanged(any())
        }

        verify(exactly = 0) {
            groupProducerService.publish(any())
        }
    }

    @Test
    fun `storeAndPublishIfChanged does not store or publish when resourceGroupID is missing`() {
        val objectId = UUID.randomUUID()
        val entraGroup =
            EntraGroup(
                objectId = objectId.toString(),
                displayName = "TestGroup",
                resourceGroupID = null,
            )

        val result = service.storeAndPublishIfChanged(entraGroup)

        assertFalse(result)

        verify(exactly = 0) {
            checksumService.checksum(any())
        }

        verify(exactly = 0) {
            groupRepository.batchUpsertReturningChanged(any())
        }

        verify(exactly = 0) {
            groupProducerService.publish(any())
        }
    }

    @Test
    fun `deleteAndPublish deletes state and publishes tombstone when objectId is valid`() {
        val objectId = UUID.randomUUID()

        every { groupRepository.deleteById(objectId) } just Runs
        every {
            groupProducerService.publishDeletedGroup(
                groupId = objectId.toString(),
                resourceGroupId = null,
                traceId = null,
            )
        } just Runs

        val result = service.deleteAndPublish(objectId.toString())

        assertTrue(result)

        verify(exactly = 1) {
            groupRepository.deleteById(objectId)
        }

        verify(exactly = 1) {
            groupProducerService.publishDeletedGroup(
                groupId = objectId.toString(),
                resourceGroupId = null,
                traceId = null,
            )
        }
    }

    @Test
    fun `deleteAndPublish does not delete or publish when objectId is invalid`() {
        val result = service.deleteAndPublish("not-a-uuid")

        assertFalse(result)

        verify(exactly = 0) {
            groupRepository.deleteById(any())
        }

        verify(exactly = 0) {
            groupProducerService.publishDeletedGroup(any(), any(), any())
        }
    }
}
