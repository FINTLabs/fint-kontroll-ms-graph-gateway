package no.novari.msgraphgateway.kafka.group

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.novari.msgraphgateway.config.ConfigGroup
import no.novari.msgraphgateway.entra.EntraStatus
import no.novari.msgraphgateway.entra.group.EntraGroup
import no.novari.msgraphgateway.group.EntraGroupMapper
import no.novari.msgraphgateway.services.group.EntraGroupCommandService
import no.novari.msgraphgateway.services.group.EntraGroupStateService
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class ResourceGroupConsumerServiceTest {
    private val configGroup = mockk<ConfigGroup>()
    private val entraGroupMapper = mockk<EntraGroupMapper>()
    private val entraGroupStateService = mockk<EntraGroupStateService>()
    private val entraGroupCommandService = mockk<EntraGroupCommandService>()

    private val service =
        ResourceGroupConsumerService(
            configGroup = configGroup,
            entraGroupMapper = entraGroupMapper,
            entraGroupStateService = entraGroupStateService,
            entraGroupCommandService = entraGroupCommandService,
        )

    @BeforeEach
    fun setUp() {
        every { entraGroupStateService.findObjectIdByResourceGroupId(any()) } returns null
    }

    @Test
    fun `process creates group stores state when resource group does not exist`() {
        val traceId = "trace-123"
        val groupId = "11111111-1111-1111-1111-111111111111"
        val resourceGroup = createResourceGroup()

        every { entraGroupCommandService.findGroupIdByResourceGroupId("12345") } returns null
        every { entraGroupCommandService.createGroup(resourceGroup) } returns
            EntraGroupCommandService.EntraGroupCommandResult(
                success = true,
                groupId = groupId,
                message = "Created Entra group",
            )
        every { entraGroupMapper.expectedFromResourceGroup(resourceGroup.copy(groupObjectId = groupId)) } returns
            EntraGroup(
                objectId = groupId,
                displayName = "TestGroup",
                resourceGroupID = 12345,
            )
        every { entraGroupStateService.storeAndPublish(any(), traceId, EntraStatus.CREATED) } returns true

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            entraGroupStateService.findObjectIdByResourceGroupId("12345")
            entraGroupCommandService.findGroupIdByResourceGroupId("12345")
            entraGroupCommandService.createGroup(resourceGroup)
        }

        verify(exactly = 1) {
            entraGroupStateService.storeAndPublish(
                match {
                    it.objectId == groupId &&
                        it.resourceGroupID == 12345L
                },
                traceId,
                EntraStatus.CREATED,
            )
        }
    }

    @Test
    fun `process creates group when local state is stale and resource group does not exist in Entra`() {
        val traceId = "trace-123"
        val staleGroupId = "11111111-1111-1111-1111-111111111111"
        val newGroupId = "22222222-2222-2222-2222-222222222222"
        val resourceGroup = createResourceGroup()

        every { entraGroupStateService.findObjectIdByResourceGroupId("12345") } returns staleGroupId
        every { entraGroupCommandService.findGroupIdByResourceGroupId("12345") } returns null
        every {
            entraGroupStateService.deleteAndPublish(
                objectId = staleGroupId,
                resourceGroupId = 12345L,
                traceId = traceId,
            )
        } returns true
        every { entraGroupCommandService.createGroup(resourceGroup) } returns
            EntraGroupCommandService.EntraGroupCommandResult(
                success = true,
                groupId = newGroupId,
                message = "Created Entra group",
            )
        every { entraGroupMapper.expectedFromResourceGroup(resourceGroup.copy(groupObjectId = newGroupId)) } returns
            EntraGroup(
                objectId = newGroupId,
                displayName = "TestGroup",
                resourceGroupID = 12345,
            )
        every { entraGroupStateService.storeAndPublish(any(), traceId, EntraStatus.CREATED) } returns true

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            entraGroupStateService.findObjectIdByResourceGroupId("12345")
            entraGroupCommandService.findGroupIdByResourceGroupId("12345")
            entraGroupStateService.deleteAndPublish(
                objectId = staleGroupId,
                resourceGroupId = 12345L,
                traceId = traceId,
            )
            entraGroupCommandService.createGroup(resourceGroup)
        }

        verify(exactly = 1) {
            entraGroupStateService.storeAndPublish(
                match {
                    it.objectId == newGroupId &&
                        it.resourceGroupID == 12345L
                },
                traceId,
                EntraStatus.CREATED,
            )
        }
    }

    @Test
    fun `process does not create group when resource group already exists in Entra`() {
        val traceId = "trace-123"
        val groupId = "11111111-1111-1111-1111-111111111111"
        val resourceGroup = createResourceGroup()

        every { entraGroupCommandService.findGroupIdByResourceGroupId("12345") } returns groupId
        every { entraGroupMapper.expectedFromResourceGroup(resourceGroup.copy(groupObjectId = groupId)) } returns
            EntraGroup(
                objectId = groupId,
                displayName = "TestGroup",
                resourceGroupID = 12345,
            )
        every { entraGroupStateService.storeAndPublish(any(), traceId, EntraStatus.CREATED) } returns true

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            entraGroupStateService.findObjectIdByResourceGroupId("12345")
            entraGroupCommandService.findGroupIdByResourceGroupId("12345")
            entraGroupStateService.storeAndPublish(any(), traceId, EntraStatus.CREATED)
        }

        verify(exactly = 0) {
            entraGroupCommandService.createGroup(any())
            entraGroupStateService.storeAndPublishIfChanged(any(), any(), any())
        }
    }

    @Test
    fun `process does not create group when duplicate lookup fails`() {
        val traceId = "trace-123"
        val resourceGroup = createResourceGroup()

        every { entraGroupCommandService.findGroupIdByResourceGroupId("12345") } throws RuntimeException("Graph error")

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            entraGroupStateService.findObjectIdByResourceGroupId("12345")
            entraGroupCommandService.findGroupIdByResourceGroupId("12345")
        }

        verify(exactly = 0) {
            entraGroupCommandService.createGroup(any())
            entraGroupStateService.storeAndPublishIfChanged(any(), any(), any())
        }
    }

    @Test
    fun `process updates group and publishes local state even when unchanged`() {
        val traceId = "trace-UPDATE-123"
        val groupId = "11111111-1111-1111-1111-111111111111"
        val resourceGroup = updateResourceGroup(groupObjectId = groupId)

        every { configGroup.allowGroupUpdate } returns true
        every { entraGroupCommandService.updateGroup(resourceGroup) } returns
            EntraGroupCommandService.EntraGroupCommandResult(
                success = true,
                groupId = groupId,
                message = "Updated Entra group",
            )
        every { entraGroupMapper.expectedFromResourceGroup(resourceGroup) } returns
            EntraGroup(
                objectId = groupId,
                displayName = "UpdatedGroup",
                resourceGroupID = 12345,
            )
        every { entraGroupStateService.storeAndPublish(any(), traceId, EntraStatus.UPDATED) } returns true

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            entraGroupCommandService.updateGroup(resourceGroup)
            entraGroupStateService.storeAndPublish(any(), traceId, EntraStatus.UPDATED)
        }

        verify(exactly = 0) {
            entraGroupStateService.storeAndPublishIfChanged(any(), any(), any())
        }
    }

    @Test
    fun `process does not call graph when create is missing resourceName`() {
        val resourceGroup = createResourceGroup(resourceName = null)

        service.process(resourceGroup, "trace-123")

        verify(exactly = 0) {
            entraGroupStateService.findObjectIdByResourceGroupId(any())
            entraGroupCommandService.findGroupIdByResourceGroupId(any())
            entraGroupCommandService.createGroup(any())
            entraGroupStateService.storeAndPublishIfChanged(any(), any(), any())
        }
    }

    @Test
    fun `process does not call graph when create has invalid resourceGroupId`() {
        val resourceGroup = createResourceGroup(id = "abc")

        service.process(resourceGroup, "trace-123")

        verify(exactly = 0) {
            entraGroupStateService.findObjectIdByResourceGroupId(any())
            entraGroupCommandService.findGroupIdByResourceGroupId(any())
            entraGroupCommandService.createGroup(any())
            entraGroupStateService.storeAndPublishIfChanged(any(), any(), any())
        }
    }

    @Test
    fun `process does not call graph when create has groupObjectId`() {
        val resourceGroup =
            createResourceGroup(
                groupObjectId = "11111111-1111-1111-1111-111111111111",
            )

        service.process(resourceGroup, "trace-123")

        verify(exactly = 0) {
            entraGroupStateService.findObjectIdByResourceGroupId(any())
            entraGroupCommandService.findGroupIdByResourceGroupId(any())
            entraGroupCommandService.createGroup(any())
            entraGroupStateService.storeAndPublishIfChanged(any(), any(), any())
        }
    }

    @Test
    fun `process deletes local state when delete group is already missing in Entra`() {
        val traceId = "trace-delete-lookup-123"
        val groupId = "11111111-1111-1111-1111-111111111111"
        val resourceGroup = deleteResourceGroup()

        every { configGroup.allowGroupDelete } returns true
        every { entraGroupCommandService.findGroupIdByResourceGroupId("12345") } returns null
        every { entraGroupStateService.findObjectIdByResourceGroupId("12345") } returns groupId
        every {
            entraGroupStateService.deleteAndPublish(
                objectId = groupId,
                resourceGroupId = 12345L,
                traceId = traceId,
            )
        } returns true

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            entraGroupCommandService.findGroupIdByResourceGroupId("12345")
            entraGroupStateService.findObjectIdByResourceGroupId("12345")
            entraGroupStateService.deleteAndPublish(
                objectId = groupId,
                resourceGroupId = 12345L,
                traceId = traceId,
            )
        }

        verify(exactly = 0) {
            entraGroupCommandService.deleteGroupById(any(), any())
        }
    }

    @Test
    fun `process does not delete local state when delete group is missing in Entra and local state is missing`() {
        val traceId = "trace-delete-lookup-123"
        val resourceGroup = deleteResourceGroup()

        every { configGroup.allowGroupDelete } returns true
        every { entraGroupCommandService.findGroupIdByResourceGroupId("12345") } returns null
        every { entraGroupStateService.findObjectIdByResourceGroupId("12345") } returns null

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            entraGroupCommandService.findGroupIdByResourceGroupId("12345")
            entraGroupStateService.findObjectIdByResourceGroupId("12345")
        }

        verify(exactly = 0) {
            entraGroupCommandService.deleteGroupById(any(), any())
            entraGroupStateService.deleteAndPublish(any(), any(), any())
        }
    }

    private fun createResourceGroup(
        id: String = "12345",
        resourceName: String? = "TestGroup",
        groupObjectId: String? = null,
    ): ResourceGroup =
        ResourceGroup(
            operation = ResourceGroupOperation.CREATE,
            id = id,
            resourceName = resourceName,
            groupObjectId = groupObjectId,
        )

    private fun deleteResourceGroup(): ResourceGroup =
        ResourceGroup(
            operation = ResourceGroupOperation.DELETE,
            id = "12345",
            resourceName = null,
            groupObjectId = null,
        )

    private fun updateResourceGroup(groupObjectId: String): ResourceGroup =
        ResourceGroup(
            operation = ResourceGroupOperation.UPDATE,
            id = "12345",
            resourceName = "UpdatedGroup",
            groupObjectId = groupObjectId,
        )
}
