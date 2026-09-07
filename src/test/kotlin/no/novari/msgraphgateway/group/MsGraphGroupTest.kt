package no.novari.msgraphgateway.group

import com.microsoft.graph.groups.delta.DeltaGetResponse
import com.microsoft.graph.models.Group
import com.microsoft.graph.models.GroupCollectionResponse
import com.microsoft.graph.models.User
import com.microsoft.graph.serviceclient.GraphServiceClient
import io.mockk.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import no.novari.msgraphgateway.config.ConfigGroup
import no.novari.msgraphgateway.config.ConfigUser
import no.novari.msgraphgateway.entra.DeltaLinkStore
import no.novari.msgraphgateway.services.group.EntraGroupMembershipSyncService
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
    private val groupMembershipSyncService = mockk<EntraGroupMembershipSyncService>(relaxed = true)
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
                groupMembershipSyncService = groupMembershipSyncService,
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
                groupMembershipSyncService = groupMembershipSyncService,
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
    fun `delta sync uses persisted link and replaces it with final link`() =
        runBlocking {
            every { deltaLinkStore.find("groups-with-members-bootstrap") } returns "true"
            every { deltaLinkStore.find("groups-with-members") } returns "persisted-delta-link"
            every { configGroup.getGroupAttributesNotMembers() } returns arrayOf("id", "displayName")
            every { configGroup.groupPagingSize } returns 999
            every {
                graphServiceClient
                    .groups()
                    .delta()
                    .withUrl("persisted-delta-link")
                    .get(any())
            } returns
                DeltaGetResponse().apply {
                    value = emptyList()
                    odataDeltaLink = "refreshed-delta-link"
                }
            coEvery {
                deltaLinkStore.createOrUpdate("groups-with-members", "refreshed-delta-link")
            } just Runs

            val service =
                MsGraphGroup(
                    configGroup = configGroup,
                    graphServiceClient = graphServiceClient,
                    groupSyncService = groupSyncService,
                    groupMembershipSyncService = groupMembershipSyncService,
                    deltaLinkStore = deltaLinkStore,
                    configUser = configUser,
                )

            service.loadDeltaLink()
            service.pullAllGroupsDelta()

            coVerify(timeout = 2_000, exactly = 1) {
                deltaLinkStore.createOrUpdate("groups-with-members", "refreshed-delta-link")
            }
            verify(exactly = 1) {
                graphServiceClient
                    .groups()
                    .delta()
                    .withUrl("persisted-delta-link")
                    .get(any())
            }
            service.shutdown()
        }

    @Test
    fun `full sync applies delta catch-up to snapshot and stores its final link`() =
        runTest {
            val changedGroup = wantedGroup("71d01c78-e5fb-4dae-8c52-2970a15c64dc", "Group-FINT", "1")
            val latestResponse =
                DeltaGetResponse().apply {
                    value = emptyList()
                    odataDeltaLink = "snapshot-boundary-link"
                }
            val catchUpResponse =
                DeltaGetResponse().apply {
                    value = listOf(changedGroup)
                    odataDeltaLink = "caught-up-delta-link"
                }
            val deltaSnapshotRunId = slot<java.util.UUID>()
            val completedSnapshotRunId = slot<java.util.UUID>()

            every { configGroup.getGroupAttributesNotMembers() } returns arrayOf("id", "displayName")
            every { configGroup.groupPagingSize } returns 999
            every {
                graphServiceClient
                    .groups()
                    .delta()
                    .withUrl(match { it.contains("deltatoken=latest") })
                    .get()
            } returns latestResponse
            every { graphServiceClient.groups().get(any()) } returns
                GroupCollectionResponse().apply { value = emptyList() }
            every {
                graphServiceClient
                    .groups()
                    .delta()
                    .withUrl("snapshot-boundary-link")
                    .get()
            } returns catchUpResponse
            every { groupSyncService.matchesConfiguredGroup(changedGroup) } returns true
            coEvery { groupSyncService.processPage(any(), any(), false) } returns 0
            every {
                groupMembershipSyncService.processDeltaPage(listOf(changedGroup), capture(deltaSnapshotRunId))
            } returns 1
            every {
                groupMembershipSyncService.completeSnapshot(
                    capture(completedSnapshotRunId),
                    initialBootstrap = true,
                    republishAll = false,
                )
            } returns EntraGroupMembershipSyncService.MembershipSnapshotResult(1, 0)
            every { groupMembershipSyncService.discardSnapshot(any()) } just Runs
            coEvery { groupSyncService.markNotSeenGroups(any(), any()) } returns 0
            coEvery { groupSyncService.finishFullImport(any()) } returns 0
            coEvery {
                deltaLinkStore.createOrUpdate("groups-with-members", "caught-up-delta-link")
            } just Runs
            coEvery {
                deltaLinkStore.createOrUpdate("groups-with-members-bootstrap", "true")
            } just Runs

            val service =
                MsGraphGroup(
                    configGroup = configGroup,
                    graphServiceClient = graphServiceClient,
                    groupSyncService = groupSyncService,
                    groupMembershipSyncService = groupMembershipSyncService,
                    deltaLinkStore = deltaLinkStore,
                    configUser = configUser,
                )

            service.startFullImport()

            assertEquals(completedSnapshotRunId.captured, deltaSnapshotRunId.captured)
            coVerifyOrder {
                groupMembershipSyncService.processDeltaPage(listOf(changedGroup), any())
                groupMembershipSyncService.completeSnapshot(any(), true, false)
                deltaLinkStore.createOrUpdate("groups-with-members", "caught-up-delta-link")
            }

            val firstRunId = completedSnapshotRunId.captured
            every {
                groupMembershipSyncService.completeSnapshot(
                    capture(completedSnapshotRunId),
                    initialBootstrap = false,
                    republishAll = false,
                )
            } returns EntraGroupMembershipSyncService.MembershipSnapshotResult(0, 0)

            service.startFullImport()

            assertNotEquals(firstRunId, completedSnapshotRunId.captured)
            assertEquals(completedSnapshotRunId.captured, deltaSnapshotRunId.captured)
        }

    @Test
    fun `startFullImport finishes full import cleanup after paging`() =
        runTest {
            val latestResponse =
                DeltaGetResponse().apply {
                    value = emptyList()
                    odataDeltaLink = "bootstrap-delta-link"
                }
            val catchUpResponse =
                DeltaGetResponse().apply {
                    value = emptyList()
                    odataDeltaLink = "new-delta-link"
                }
            val groupListResponse = GroupCollectionResponse().apply { value = emptyList() }

            every { configGroup.getGroupAttributesNotMembers() } returns arrayOf("id", "displayName")
            every { configGroup.groupPagingSize } returns 999

            every {
                graphServiceClient
                    .groups()
                    .delta()
                    .withUrl(match { it.contains("deltatoken=latest") })
                    .get()
            } returns latestResponse
            every {
                graphServiceClient.groups().get(any())
            } returns groupListResponse
            every {
                graphServiceClient
                    .groups()
                    .delta()
                    .withUrl("bootstrap-delta-link")
                    .get()
            } returns catchUpResponse

            coEvery {
                deltaLinkStore.createOrUpdate("groups-with-members", "new-delta-link")
            } just Runs
            coEvery {
                groupSyncService.processPage(any(), any(), false)
            } returns 0

            val cutoffSlot = slot<Instant>()
            val markNotSeenCutoffSlot = slot<Instant>()

            coEvery {
                groupSyncService.markNotSeenGroups(capture(markNotSeenCutoffSlot), any())
            } returns 0

            coEvery {
                groupSyncService.finishFullImport(capture(cutoffSlot))
            } returns 3
            every {
                groupMembershipSyncService.completeSnapshot(any(), initialBootstrap = true, republishAll = false)
            } returns EntraGroupMembershipSyncService.MembershipSnapshotResult(0, 0)
            every { groupMembershipSyncService.discardSnapshot(any()) } just Runs
            coEvery {
                deltaLinkStore.createOrUpdate("groups-with-members-bootstrap", "true")
            } just Runs

            val service =
                MsGraphGroup(
                    configGroup = configGroup,
                    graphServiceClient = graphServiceClient,
                    groupSyncService = groupSyncService,
                    groupMembershipSyncService = groupMembershipSyncService,
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
                groupMembershipSyncService.completeSnapshot(any(), true, false)
                groupSyncService.markNotSeenGroups(any(), any())
                groupSyncService.finishFullImport(any())
                deltaLinkStore.createOrUpdate("groups-with-members", "new-delta-link")
                deltaLinkStore.createOrUpdate("groups-with-members-bootstrap", "true")
            }

            assertTrue(markNotSeenCutoffSlot.captured >= before)
            assertTrue(markNotSeenCutoffSlot.captured <= after)
            assertTrue(cutoffSlot.captured >= before)
            assertTrue(cutoffSlot.captured <= after)
            assertEquals(markNotSeenCutoffSlot.captured, cutoffSlot.captured)
        }

    @Test
    fun `startFullImport never reconciles missing memberships without final delta link`() =
        runTest {
            val latestResponse =
                DeltaGetResponse().apply {
                    value = emptyList()
                    odataDeltaLink = "bootstrap-delta-link"
                }
            val catchUpResponse = DeltaGetResponse().apply { value = emptyList() }

            every { configGroup.getGroupAttributesNotMembers() } returns arrayOf("id", "displayName")
            every { configGroup.groupPagingSize } returns 999
            every {
                graphServiceClient
                    .groups()
                    .delta()
                    .withUrl(match { it.contains("deltatoken=latest") })
                    .get()
            } returns latestResponse
            every { graphServiceClient.groups().get(any()) } returns
                GroupCollectionResponse().apply {
                    value = emptyList()
                }
            every {
                graphServiceClient
                    .groups()
                    .delta()
                    .withUrl("bootstrap-delta-link")
                    .get()
            } returns catchUpResponse
            coEvery { groupSyncService.processPage(any(), any(), false) } returns 0

            val service =
                MsGraphGroup(
                    configGroup = configGroup,
                    graphServiceClient = graphServiceClient,
                    groupSyncService = groupSyncService,
                    groupMembershipSyncService = groupMembershipSyncService,
                    deltaLinkStore = deltaLinkStore,
                    configUser = configUser,
                )

            assertThrows(IllegalStateException::class.java) {
                runBlocking { service.startFullImport() }
            }

            verify(exactly = 0) { groupMembershipSyncService.completeSnapshot(any(), any(), any()) }
            verify(exactly = 1) { groupMembershipSyncService.discardSnapshot(any()) }
            coVerify(exactly = 0) { deltaLinkStore.createOrUpdate(any(), any()) }
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
                        groupMembershipSyncService = groupMembershipSyncService,
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
            withTimeout(2_000) {
                while (fullImportRequested(service).get()) delay(10)
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
