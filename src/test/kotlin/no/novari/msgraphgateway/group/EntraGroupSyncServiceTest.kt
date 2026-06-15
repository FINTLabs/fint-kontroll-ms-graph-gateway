package no.novari.msgraphgateway.group

import io.mockk.*
import kotlinx.coroutines.test.runTest
import no.novari.msgraphgateway.config.ConfigGroup
import no.novari.msgraphgateway.kafka.group.GroupProducerService
import no.novari.msgraphgateway.repository.group.GroupRepository
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
}
