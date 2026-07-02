package no.novari.msgraphgateway.group

import com.microsoft.graph.groups.delta.DeltaGetResponse
import com.microsoft.graph.models.Group
import com.microsoft.graph.models.GroupCollectionResponse
import com.microsoft.graph.models.User
import com.microsoft.graph.serviceclient.GraphServiceClient
import io.mockk.*
import kotlinx.coroutines.test.runTest
import no.novari.msgraphgateway.config.ConfigGroup
import no.novari.msgraphgateway.config.ConfigUser
import no.novari.msgraphgateway.entra.DeltaLinkStore
import no.novari.msgraphgateway.services.group.EntraGroupSyncService
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Instant

class MsGraphGroupTest {
    private val configGroup = mockk<ConfigGroup>()
    private val configUser = mockk<ConfigUser>()
    private val graphServiceClient = mockk<GraphServiceClient>(relaxed = true)
    private val groupSyncService = mockk<EntraGroupSyncService>()
    private val deltaLinkStore = mockk<DeltaLinkStore>()

    @Test
    fun `getEntraUserWithGroups fetches groups through transitive memberOf`() {
        val localConfigGroup =
            ConfigGroup(minNotSeenCount = 7).apply {
                suffix = "-FINT"
                resourceGroupIdAttribute = "extension_resourceGroupId"
            }
        val localConfigUser = ConfigUser()
        val service =
            MsGraphGroup(
                configGroup = localConfigGroup,
                graphServiceClient = graphServiceClient,
                groupSyncService = groupSyncService,
                deltaLinkStore = deltaLinkStore,
                configUser = localConfigUser,
            )

        val user =
            User().apply {
                id = "user-1"
                userPrincipalName = "user@example.org"
                accountEnabled = true
            }
        val matchingGroup =
            Group().apply {
                id = "group-1"
                displayName = "Kontroll-FINT"
                securityEnabled = true
                additionalData["extension_resourceGroupId"] = "12345"
            }
        val nonSecurityGroup =
            Group().apply {
                id = "group-2"
                displayName = "Other-FINT"
                securityEnabled = false
                additionalData["extension_resourceGroupId"] = "67890"
            }
        val missingResourceGroupId =
            Group().apply {
                id = "group-3"
                displayName = "Missing-FINT"
                securityEnabled = true
            }
        val response =
            GroupCollectionResponse().apply {
                value = listOf(matchingGroup, nonSecurityGroup, missingResourceGroupId)
            }

        every { graphServiceClient.users().byUserId("user-1").get(any()) } returns user
        every {
            graphServiceClient
                .users()
                .byUserId("user-1")
                .transitiveMemberOf()
                .graphGroup()
                .get(any())
        } returns response

        val result = service.getEntraUserWithGroups("user-1")

        assertEquals("user-1", result.user?.userObjectId)
        assertEquals(listOf("group-1"), result.groups.map { it.objectId })
        assertEquals(listOf(12345L), result.groups.map { it.resourceGroupID })
    }

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
                    configUser = configUser,
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
