package no.novari.msgraphgateway.group

import com.microsoft.graph.groups.delta.DeltaGetResponse
import com.microsoft.graph.serviceclient.GraphServiceClient
import io.mockk.*
import kotlinx.coroutines.test.runTest
import no.novari.msgraphgateway.config.ConfigGroup
import no.novari.msgraphgateway.entra.DeltaLinkStore
import no.novari.msgraphgateway.services.EntraGroupSyncService
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Instant

class MsGraphGroupTest {
    private val configGroup = mockk<ConfigGroup>()
    private val graphServiceClient = mockk<GraphServiceClient>(relaxed = true)
    private val groupSyncService = mockk<EntraGroupSyncService>()
    private val deltaLinkStore = mockk<DeltaLinkStore>()

    @Test
    fun `startFullImport finishes full import cleanup after paging`() =
        runTest {
            val response =
                DeltaGetResponse().apply {
                    value = emptyList()
                    odataDeltaLink = "new-delta-link"
                }

            every { configGroup.getGroupAttributesNotMembers() } returns arrayOf("id", "displayName")
            every { configGroup.groupPagingSize } returns 999

            every {
                graphServiceClient.groups().delta().get(any())
            } returns response

            coEvery {
                deltaLinkStore.createOrUpdate("groups", "new-delta-link")
            } just Runs

            val cutoffSlot = slot<Instant>()

            coEvery {
                groupSyncService.finishFullImport(capture(cutoffSlot))
            } returns 3

            val service =
                MsGraphGroup(
                    configGroup = configGroup,
                    graphServiceClient = graphServiceClient,
                    groupSyncService = groupSyncService,
                    deltaLinkStore = deltaLinkStore,
                )

            val before = Instant.now()

            service.startFullImport(republishAll = false)

            val after = Instant.now()

            coVerify(exactly = 1) {
                groupSyncService.finishFullImport(any())
            }

            assertTrue(cutoffSlot.captured >= before)
            assertTrue(cutoffSlot.captured <= after)
        }
}
