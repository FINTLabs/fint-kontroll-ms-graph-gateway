package no.novari.msgraphgateway.group

import com.microsoft.graph.groups.delta.DeltaGetResponse
import com.microsoft.graph.models.Group
import com.microsoft.graph.models.GroupCollectionResponse
import com.microsoft.graph.models.User
import com.microsoft.graph.serviceclient.GraphServiceClient
import io.mockk.*
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import no.novari.msgraphgateway.config.ConfigGroup
import no.novari.msgraphgateway.config.ConfigUser
import no.novari.msgraphgateway.entra.DeltaLinkStore
import no.novari.msgraphgateway.services.group.EntraGroupSyncService
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

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
    fun `getEntraUserWithGroups follows configured group filter mode`() {
        val localConfigGroup =
            ConfigGroup(minNotSeenCount = 7).apply {
                prefix = "PRE-"
                suffix = "-SUF"
                resourceGroupIdAttribute = "extension_resourceGroupId"
            }
        val service =
            MsGraphGroup(
                configGroup = localConfigGroup,
                graphServiceClient = graphServiceClient,
                groupSyncService = groupSyncService,
                deltaLinkStore = deltaLinkStore,
                configUser = ConfigUser(),
            )
        val user =
            User().apply {
                id = "user-1"
                userPrincipalName = "user@example.org"
                accountEnabled = true
            }
        val response =
            GroupCollectionResponse().apply {
                value =
                    listOf(
                        wantedGroup("prefix-only", "PRE-Group", "1"),
                        wantedGroup("suffix-only", "Group-SUF", "2"),
                        wantedGroup("both", "PRE-Group-SUF", "3"),
                        wantedGroup("neither", "Group", "4"),
                    )
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

        localConfigGroup.filterMode = ConfigGroup.FilterMode.PREFIX
        assertEquals(
            listOf("prefix-only", "both"),
            service.getEntraUserWithGroups("user-1").groups.map { it.objectId },
        )

        localConfigGroup.filterMode = ConfigGroup.FilterMode.SUFFIX
        assertEquals(
            listOf("suffix-only", "both"),
            service.getEntraUserWithGroups("user-1").groups.map { it.objectId },
        )

        localConfigGroup.filterMode = ConfigGroup.FilterMode.BOTH
        assertEquals(
            listOf("both"),
            service.getEntraUserWithGroups("user-1").groups.map { it.objectId },
        )

        localConfigGroup.filterMode = ConfigGroup.FilterMode.NONE
        assertEquals(
            listOf("prefix-only", "suffix-only", "both", "neither"),
            service.getEntraUserWithGroups("user-1").groups.map { it.objectId },
        )
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
            val markNotSeenCutoffSlot = slot<Instant>()

            coEvery {
                groupSyncService.markNotSeenGroups(capture(markNotSeenCutoffSlot), any())
            } returns 0

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
                groupSyncService.markNotSeenGroups(any(), any())
            }

            coVerify(exactly = 1) {
                groupSyncService.finishFullImport(any())
            }

            coVerifyOrder {
                groupSyncService.markNotSeenGroups(any(), any())
                groupSyncService.finishFullImport(any())
            }

            assertTrue(markNotSeenCutoffSlot.captured >= before)
            assertTrue(markNotSeenCutoffSlot.captured <= after)
            assertTrue(cutoffSlot.captured >= before)
            assertTrue(cutoffSlot.captured <= after)
            assertEquals(markNotSeenCutoffSlot.captured, cutoffSlot.captured)
        }

    @Test
    fun `pullAllGroupsDelta starts pending full import when no sync is running`() =
        runBlocking {
            val service =
                spyk(
                    MsGraphGroup(
                        configGroup = configGroup,
                        graphServiceClient = graphServiceClient,
                        groupSyncService = groupSyncService,
                        deltaLinkStore = deltaLinkStore,
                        configUser = configUser,
                    ),
                )

            coEvery { service.startFullImport(any()) } returns Unit
            fullImportRequested(service).set(true)

            service.pullAllGroupsDelta()

            coVerify(timeout = 2_000, exactly = 1) {
                service.startFullImport(false)
            }
            verify(exactly = 0) {
                graphServiceClient.groups().delta().get(any())
            }
            assertFalse(fullImportRequested(service).get())
        }

    private fun wantedGroup(
        id: String,
        displayName: String,
        resourceGroupId: String,
    ): Group =
        Group().apply {
            this.id = id
            this.displayName = displayName
            securityEnabled = true
            additionalData["extension_resourceGroupId"] = resourceGroupId
        }

    private fun fullImportRequested(service: MsGraphGroup): AtomicBoolean {
        val field = service.javaClass.getDeclaredField("fullImportRequested")
        field.isAccessible = true
        return field.get(service) as AtomicBoolean
    }
}
