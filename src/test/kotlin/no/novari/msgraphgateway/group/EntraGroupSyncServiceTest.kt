package no.novari.msgraphgateway.group

import com.microsoft.graph.models.Group
import io.mockk.*
import kotlinx.coroutines.test.runTest
import no.novari.msgraphgateway.config.ConfigGroup
import no.novari.msgraphgateway.kafka.group.GroupProducerService
import no.novari.msgraphgateway.repository.group.GroupRepository
import no.novari.msgraphgateway.services.Checksum
import no.novari.msgraphgateway.services.ChecksumService
import no.novari.msgraphgateway.services.EntraGroupSyncService
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.UUID

class EntraGroupSyncServiceTest {
    private val groupRepository = mockk<GroupRepository>()
    private val checksumService = mockk<ChecksumService>()
    private val producer = mockk<GroupProducerService>()
    private val configGroup = mockk<ConfigGroup>()

    private val service =
        EntraGroupSyncService(
            groupRepository = groupRepository,
            checksumService = checksumService,
            producer = producer,
            configGroup = configGroup,
        )

    private fun group(
        id: String,
        displayName: String,
        additionalData: Map<String, Any>,
    ): Group =
        Group().apply {
            this.id = id
            this.displayName = displayName
            this.additionalData.putAll(additionalData)
        }

    @Test
    fun `finishFullImport deletes stale groups and publishes deleted events`() =
        runTest {
            val cutoff = Instant.parse("2026-06-15T10:00:00Z")
            val id1 = UUID.randomUUID()
            val id2 = UUID.randomUUID()

            every { configGroup.minNotSeenCount } returns 2

            coEvery {
                groupRepository.findStaleObjectIdsWithNotSeenCountGreaterThan(cutoff, 2)
            } returns listOf(id1, id2)

            coEvery {
                groupRepository.deleteByIdsReturningObjectIds(listOf(id1, id2))
            } returns listOf(id1, id2)

            coEvery { producer.publishDeletedGroup(any()) } just Runs

            val result = service.finishFullImport(cutoff)

            assertEquals(2, result)

            coVerify(exactly = 1) {
                groupRepository.findStaleObjectIdsWithNotSeenCountGreaterThan(cutoff, 2)
            }

            coVerify(exactly = 1) {
                groupRepository.deleteByIdsReturningObjectIds(listOf(id1, id2))
            }

            coVerify(exactly = 1) {
                producer.publishDeletedGroup(id1.toString())
            }

            coVerify(exactly = 1) {
                producer.publishDeletedGroup(id2.toString())
            }
        }

    @Test
    fun `finishFullImport returns zero when no stale groups are found`() =
        runTest {
            val cutoff = Instant.parse("2026-06-15T10:00:00Z")

            every { configGroup.minNotSeenCount } returns 2

            coEvery {
                groupRepository.findStaleObjectIdsWithNotSeenCountGreaterThan(cutoff, 2)
            } returns emptyList()

            val result = service.finishFullImport(cutoff)

            assertEquals(0, result)

            coVerify(exactly = 0) {
                groupRepository.deleteByIdsReturningObjectIds(any())
            }

            coVerify(exactly = 0) {
                producer.publishDeletedGroup(any())
            }
        }

    @Test
    fun `processPage only publishes groups matching prefix and resource group attribute`() =
        runTest {
            val validId = UUID.randomUUID()

            val validGroup =
                group(
                    id = validId.toString(),
                    displayName = "PREFIX_Test",
                    additionalData = mapOf("extension_resourceGroupId" to "123456"),
                )

            val wrongSuffix =
                group(
                    id = UUID.randomUUID().toString(),
                    displayName = "OTHER_Test",
                    additionalData = mapOf("extension_resourceGroupId" to "123456"),
                )

            val missingAttribute =
                group(
                    id = UUID.randomUUID().toString(),
                    displayName = "PREFIX_Other_GROUP_NO_ResID",
                    additionalData = emptyMap(),
                )

            every { configGroup.prefix } returns "PREFIX_"
            every { configGroup.suffix } returns null
            every { configGroup.filterMode } returns ConfigGroup.FilterMode.PREFIX
            every { configGroup.resourceGroupIdAttribute } returns "extension_resourceGroupId"

            val checksum = Checksum("checksum".toByteArray())

            every {
                checksumService.checksum(any())
            } returns checksum
            coEvery {
                groupRepository.batchUpsertReturningChanged(any())
            } returns setOf(validId)

            coEvery { producer.publish(any()) } just Runs

            val result =
                service.processPage(
                    groups = listOf(validGroup, wrongSuffix, missingAttribute),
                    notSeenIncremented = mutableSetOf(),
                    republishAll = false,
                )

            assertEquals(1, result)

            coVerify(exactly = 1) {
                groupRepository.batchUpsertReturningChanged(
                    match { rows -> rows.size == 1 && rows.first().objectId == validId },
                )
            }

            coVerify(exactly = 1) {
                producer.publish(any())
            }
        }

    @Test
    fun `processPage only publishes groups matching suffix and resource group attribute`() =
        runTest {
            val validId = UUID.randomUUID()

            val validGroup =
                group(
                    id = validId.toString(),
                    displayName = "Test_SUFFIX",
                    additionalData = mapOf("extension_resourceGroupId" to "123456"),
                )

            val wrongSuffix =
                group(
                    id = UUID.randomUUID().toString(),
                    displayName = "Test_OTHER",
                    additionalData = mapOf("extension_resourceGroupId" to "123456"),
                )

            val missingAttribute =
                group(
                    id = UUID.randomUUID().toString(),
                    displayName = "Other_No_ResID_SUFFIX",
                    additionalData = emptyMap(),
                )

            every { configGroup.prefix } returns null
            every { configGroup.suffix } returns "_SUFFIX"
            every { configGroup.filterMode } returns ConfigGroup.FilterMode.SUFFIX
            every { configGroup.resourceGroupIdAttribute } returns "extension_resourceGroupId"

            val checksum = Checksum("checksum".toByteArray())

            every {
                checksumService.checksum(any())
            } returns checksum
            coEvery {
                groupRepository.batchUpsertReturningChanged(any())
            } returns setOf(validId)

            coEvery { producer.publish(any()) } just Runs

            val result =
                service.processPage(
                    groups = listOf(validGroup, wrongSuffix, missingAttribute),
                    notSeenIncremented = mutableSetOf(),
                    republishAll = false,
                )

            assertEquals(1, result)

            coVerify(exactly = 1) {
                groupRepository.batchUpsertReturningChanged(
                    match { rows -> rows.size == 1 && rows.first().objectId == validId },
                )
            }

            coVerify(exactly = 1) {
                producer.publish(any())
            }
        }
}
