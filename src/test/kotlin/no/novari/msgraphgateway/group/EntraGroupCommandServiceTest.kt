package no.novari.msgraphgateway.group

import com.microsoft.graph.groups.GroupsRequestBuilder
import com.microsoft.graph.groups.item.GroupItemRequestBuilder
import com.microsoft.graph.models.Group
import com.microsoft.graph.models.GroupCollectionResponse
import com.microsoft.graph.serviceclient.GraphServiceClient
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import no.novari.msgraphgateway.config.ConfigGroup
import no.novari.msgraphgateway.kafka.group.ResourceGroup
import no.novari.msgraphgateway.kafka.group.ResourceGroupOperation
import no.novari.msgraphgateway.services.group.EntraGroupCommandService
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class EntraGroupCommandServiceTest {
    private val graphServiceClient = mockk<GraphServiceClient>()
    private val groupsRequestBuilder = mockk<GroupsRequestBuilder>()
    private val entraGroupMapper = mockk<EntraGroupMapper>()

    private val configGroup =
        ConfigGroup(
            resourceGroupIdAttribute = "extension_resourceGroupId",
            minNotSeenCount = 2,
        )

    private val service =
        EntraGroupCommandService(
            graphServiceClient = graphServiceClient,
            configGroup = configGroup,
            entraGroupMapper = entraGroupMapper,
        )

    private fun mockGroupsResponsePages(
        firstPageGroups: List<Group>,
        secondPageGroups: List<Group>,
    ) {
        val secondPageBuilder = mockk<GroupsRequestBuilder>()

        every {
            groupsRequestBuilder.get(any())
        } returns
            GroupCollectionResponse().apply {
                value = firstPageGroups
                odataNextLink = "next-link"
            }

        every {
            groupsRequestBuilder.withUrl("next-link")
        } returns secondPageBuilder

        every {
            secondPageBuilder.get()
        } returns
            GroupCollectionResponse().apply {
                value = secondPageGroups
                odataNextLink = null
            }
    }

    @BeforeEach
    fun setUp() {
        every { graphServiceClient.groups() } returns groupsRequestBuilder
    }

    @Test
    fun `createGroup creates Entra group with suffix and uniqueName and resource group attribute`() {
        val slot = slot<Group>()

        every {
            groupsRequestBuilder.post(capture(slot))
        } returns
            Group().apply {
                id = "group-123"
            }

        every {
            entraGroupMapper.buildDisplayName(any())
        } returns "TestGroup_SUFFIX"

        every {
            entraGroupMapper.buildMailNickname(any())
        } returns "testgroup-suffix"

        configGroup.filterMode = ConfigGroup.FilterMode.SUFFIX
        configGroup.suffix = "_SUFFIX"

        val result =
            service.createGroup(
                ResourceGroup(
                    operation = ResourceGroupOperation.CREATE,
                    id = "12345",
                    resourceName = "TestGroup",
                    groupObjectId = null,
                ),
            )

        val group = slot.captured

        assertEquals("TestGroup_SUFFIX", group.displayName)
        assertEquals(false, group.mailEnabled)
        assertEquals(true, group.securityEnabled)
        assertEquals("testgroup-suffix", group.mailNickname)
        assertEquals("12345", group.additionalData["extension_resourceGroupId"])
        assertEquals("12345", group.uniqueName)
        assertEquals(true, result.success)
        assertEquals("group-123", result.groupId)
        assertEquals("Created Entra group", result.message)

        verify(exactly = 1) {
            groupsRequestBuilder.post(any<Group>())
            entraGroupMapper.buildDisplayName(any())
            entraGroupMapper.buildMailNickname(any())
        }
    }

    @Test
    fun `updateGroup patches Entra group when group id exists`() {
        val groupId = "group-123"
        val groupItemRequestBuilder = mockk<GroupItemRequestBuilder>()
        val slot = slot<Group>()

        every { groupsRequestBuilder.byGroupId(groupId) } returns groupItemRequestBuilder
        every { groupItemRequestBuilder.patch(capture(slot)) } returns Group()
        every {
            entraGroupMapper.buildDisplayName(any())
        } returns "UpdatedGroup_SUFFIX"

        configGroup.filterMode = ConfigGroup.FilterMode.SUFFIX
        configGroup.suffix = "_SUFFIX"

        val result =
            service.updateGroup(
                ResourceGroup(
                    operation = ResourceGroupOperation.UPDATE,
                    id = "12345",
                    resourceName = "UpdatedGroup",
                    groupObjectId = groupId,
                ),
            )

        val group = slot.captured

        assertEquals("UpdatedGroup_SUFFIX", group.displayName)
        assertEquals("12345", group.additionalData["extension_resourceGroupId"])
        assertEquals(true, result.success)
        assertEquals(groupId, result.groupId)
        assertEquals("Updated Entra group", result.message)

        verify(exactly = 1) {
            groupsRequestBuilder.byGroupId(groupId)
            groupItemRequestBuilder.patch(any<Group>())
            entraGroupMapper.buildDisplayName(any())
        }
    }

    @Test
    fun `updateGroup does nothing when group id is missing`() {
        val result =
            service.updateGroup(
                ResourceGroup(
                    operation = ResourceGroupOperation.UPDATE,
                    id = "12345",
                    resourceName = "UpdatedGroup",
                    groupObjectId = null,
                ),
            )

        assertFalse(result.success)
        assertNull(result.groupId)
        assertEquals("Missing identityProviderGroupObjectId", result.message)

        verify(exactly = 0) {
            groupsRequestBuilder.byGroupId(any())
        }
    }

    @Test
    fun `deleteGroup deletes group when found by resource group id`() {
        val groupItemRequestBuilder = mockk<GroupItemRequestBuilder>()

        mockGroupsResponse(
            listOf(
                group(
                    id = "group-1",
                    displayName = "TestGroup_SUFFIX",
                ),
            ),
        )

        every { groupsRequestBuilder.byGroupId("group-1") } returns groupItemRequestBuilder
        every { groupItemRequestBuilder.delete() } returns Unit

        service.deleteGroup("12345")

        verify(exactly = 1) {
            groupItemRequestBuilder.delete()
        }
    }

    @Test
    fun `deleteGroup does nothing when group is not found`() {
        mockGroupsResponse(emptyList())

        service.deleteGroup("12345")

        verify(exactly = 0) {
            groupsRequestBuilder.byGroupId(any())
        }
    }

    @Test
    fun `deleteGroup does not delete when multiple groups are found`() {
        mockGroupsResponse(
            listOf(
                group("group-1", "Test group 1"),
                group("group-2", "Test group 2"),
            ),
        )

        service.deleteGroup("12345")

        verify(exactly = 0) {
            groupsRequestBuilder.byGroupId(any())
        }
    }

    @Test
    fun `findGroupIdByResourceGroupId returns null when no groups are found`() {
        mockGroupsResponse(emptyList())

        val result = service.findGroupIdByResourceGroupId("12345")

        assertNull(result)
    }

    @Test
    fun `findGroupIdByResourceGroupId returns id when one group is found`() {
        mockGroupsResponse(
            listOf(
                group(
                    id = "group-1",
                    displayName = "Test group",
                ),
            ),
        )

        val result = service.findGroupIdByResourceGroupId("12345")

        assertEquals("group-1", result)
    }

    @Test
    fun `findGroupIdByResourceGroupId throws when multiple groups are found`() {
        mockGroupsResponse(
            listOf(
                group("group-1", "Test group 1"),
                group("group-2", "Test group 2"),
                group("group-3", "Test group 3"),
            ),
        )

        assertThrows<IllegalStateException> {
            service.findGroupIdByResourceGroupId("12345")
        }
    }

    @Test
    fun `findGroupIdByResourceGroupId returns null when resourceGroupId is blank`() {
        val result = service.findGroupIdByResourceGroupId("")

        assertNull(result)
    }

    private fun mockGroupsResponse(groups: List<Group>) {
        every { graphServiceClient.groups() } returns groupsRequestBuilder

        every {
            groupsRequestBuilder.get(any())
        } returns
            GroupCollectionResponse().apply {
                value = groups
            }
    }

    private fun group(
        id: String,
        displayName: String,
    ): Group =
        Group().apply {
            this.id = id
            this.displayName = displayName
        }

    @Test
    fun `findGroupIdByResourceGroupId reads all pages and throws when duplicate is on next page`() {
        mockGroupsResponsePages(
            firstPageGroups =
                listOf(
                    group("group-1", "Test group 1"),
                ),
            secondPageGroups =
                listOf(
                    group("group-2", "Test group 2"),
                ),
        )

        assertThrows<IllegalStateException> {
            service.findGroupIdByResourceGroupId("12345")
        }
    }
}
