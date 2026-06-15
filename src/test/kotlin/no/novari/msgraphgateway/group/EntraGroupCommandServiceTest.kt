package no.novari.msgraphgateway.group

import com.microsoft.graph.groups.GroupsRequestBuilder
import com.microsoft.graph.models.Group
import com.microsoft.graph.models.GroupCollectionResponse
import com.microsoft.graph.serviceclient.GraphServiceClient
import io.mockk.every
import io.mockk.mockk
import no.novari.msgraphgateway.config.ConfigGroup
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class EntraGroupCommandServiceTest {
    private val graphServiceClient = mockk<GraphServiceClient>()
    private val groupsRequestBuilder = mockk<GroupsRequestBuilder>()

    private val configGroup =
        ConfigGroup(
            resourceGroupIdAttribute = "extension_resourceGroupId",
        )

    private val service =
        EntraGroupCommandService(
            graphServiceClient = graphServiceClient,
            configGroup = configGroup,
        )

    @Test
    fun `findGroupIdByResourceGroupId returns null when no groups are found`() {
        mockGroupsResponse(emptyList())

        val result = service.findGroupIdByResourceGroupId("rg-1")

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

        val result = service.findGroupIdByResourceGroupId("rg-1")

        assertEquals("group-1", result)
    }

    @Test
    fun `findGroupIdByResourceGroupId returns first id when multiple groups are found`() {
        mockGroupsResponse(
            listOf(
                group("group-1", "Test group 1"),
                group("group-2", "Test group 2"),
                group("group-3", "Test group 3"),
            ),
        )

        val result = service.findGroupIdByResourceGroupId("rg-1")

        assertEquals("group-1", result)
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
}
