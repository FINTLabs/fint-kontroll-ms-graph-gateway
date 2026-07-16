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
    private val groupProducerService = mockk<GroupProducerService>(relaxed = true)

    private val service =
        ResourceGroupConsumerService(
            configGroup = configGroup,
            entraGroupMapper = entraGroupMapper,
            entraGroupStateService = entraGroupStateService,
            entraGroupCommandService = entraGroupCommandService,
            groupProducerService = groupProducerService,
        )

    @BeforeEach
    fun setUp() {
        every { entraGroupStateService.findObjectIdByResourceGroupId(any()) } returns null
        every { entraGroupStateService.findResourceGroupIdByObjectId(any()) } returns null
        every { entraGroupStateService.isUnchanged(any()) } returns false
    }

    @Test
    fun `process publishes error response when resource group payload is null`() {
        val traceId = "trace-null-123"

        service.process(null, traceId)

        verify(exactly = 1) {
            groupProducerService.publishResourceGroupResponse(
                key = traceId,
                objectId = null,
                displayName = null,
                resourceGroupId = null,
                traceId = traceId,
                status = EntraStatus.ERROR,
            )
        }
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
        every { entraGroupMapper.expectedFromResourceGroup(resourceGroup.copy(idpGroupObjectId = groupId)) } returns
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
        every { entraGroupMapper.expectedFromResourceGroup(resourceGroup.copy(idpGroupObjectId = newGroupId)) } returns
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
        every { entraGroupMapper.expectedFromResourceGroup(resourceGroup.copy(idpGroupObjectId = groupId)) } returns
            EntraGroup(
                objectId = groupId,
                displayName = "TestGroup",
                resourceGroupID = 12345,
            )
        every { entraGroupStateService.storeAndPublish(any(), traceId, EntraStatus.NO_CHANGES) } returns true

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            entraGroupStateService.findObjectIdByResourceGroupId("12345")
            entraGroupCommandService.findGroupIdByResourceGroupId("12345")
            entraGroupStateService.storeAndPublish(any(), traceId, EntraStatus.NO_CHANGES)
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
    fun `process publishes error response when create fails in Entra`() {
        val traceId = "trace-create-error-123"
        val resourceGroup = createResourceGroup()

        every { entraGroupCommandService.findGroupIdByResourceGroupId("12345") } returns null
        every { entraGroupCommandService.createGroup(resourceGroup) } returns
            EntraGroupCommandService.EntraGroupCommandResult(
                success = false,
                message = "Failed creating Entra group",
                error = RuntimeException("Graph error"),
            )

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            groupProducerService.publishResourceGroupResponse(
                key = "12345",
                objectId = null,
                displayName = "TestGroup",
                resourceGroupId = 12345L,
                traceId = traceId,
                status = EntraStatus.ERROR,
            )
        }

        verify(exactly = 0) {
            entraGroupStateService.storeAndPublish(any(), any(), any())
            entraGroupStateService.storeAndPublishIfChanged(any(), any(), any())
        }
    }

    @Test
    fun `process updates group and publishes local state when changed`() {
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
    fun `process publishes no changes when update is unchanged`() {
        val traceId = "trace-UPDATE-NO-CHANGES-123"
        val groupId = "11111111-1111-1111-1111-111111111111"
        val resourceGroup = updateResourceGroup(groupObjectId = groupId)
        val expectedGroup =
            EntraGroup(
                objectId = groupId,
                displayName = "UpdatedGroup",
                resourceGroupID = 12345,
            )

        every { configGroup.allowGroupUpdate } returns true
        every { entraGroupMapper.expectedFromResourceGroup(resourceGroup) } returns expectedGroup
        every { entraGroupStateService.isUnchanged(expectedGroup) } returns true

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            entraGroupStateService.isUnchanged(expectedGroup)
            groupProducerService.publishResourceGroupResponse(
                key = groupId,
                objectId = groupId,
                displayName = "UpdatedGroup",
                resourceGroupId = 12345L,
                traceId = traceId,
                status = EntraStatus.NO_CHANGES,
            )
        }

        verify(exactly = 0) {
            entraGroupCommandService.updateGroup(any())
            entraGroupStateService.storeAndPublish(any(), any(), any())
            entraGroupStateService.storeAndPublishIfChanged(any(), any(), any())
        }
    }

    @Test
    fun `process does not update when group object id belongs to another resource group id`() {
        val traceId = "trace-UPDATE-WRONG-RESOURCE-ID-123"
        val groupId = "11111111-1111-1111-1111-111111111111"
        val resourceGroup = updateResourceGroup(id = "67890", groupObjectId = groupId)

        every { configGroup.allowGroupUpdate } returns true
        every { entraGroupStateService.findResourceGroupIdByObjectId(groupId) } returns 12345L

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            groupProducerService.publishResourceGroupResponse(
                key = groupId,
                objectId = groupId,
                displayName = "UpdatedGroup",
                resourceGroupId = 67890L,
                traceId = traceId,
                status = EntraStatus.ERROR,
            )
        }

        verify(exactly = 0) {
            entraGroupMapper.expectedFromResourceGroup(any())
            entraGroupCommandService.updateGroup(any())
            entraGroupStateService.storeAndPublish(any(), any(), any())
            entraGroupStateService.storeAndPublishIfChanged(any(), any(), any())
        }
    }

    @Test
    fun `process does not update when resource group id belongs to another group object id`() {
        val traceId = "trace-UPDATE-WRONG-GROUP-ID-123"
        val incomingGroupId = "11111111-1111-1111-1111-111111111111"
        val storedGroupId = "22222222-2222-2222-2222-222222222222"
        val resourceGroup = updateResourceGroup(groupObjectId = incomingGroupId)

        every { configGroup.allowGroupUpdate } returns true
        every { entraGroupStateService.findObjectIdByResourceGroupId("12345") } returns storedGroupId

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            groupProducerService.publishResourceGroupResponse(
                key = incomingGroupId,
                objectId = incomingGroupId,
                displayName = "UpdatedGroup",
                resourceGroupId = 12345L,
                traceId = traceId,
                status = EntraStatus.ERROR,
            )
        }

        verify(exactly = 0) {
            entraGroupMapper.expectedFromResourceGroup(any())
            entraGroupCommandService.updateGroup(any())
            entraGroupStateService.storeAndPublish(any(), any(), any())
            entraGroupStateService.storeAndPublishIfChanged(any(), any(), any())
        }
    }

    @Test
    fun `process publishes error status when update fails in Entra`() {
        val traceId = "trace-UPDATE-FAILED-123"
        val groupId = "11111111-1111-1111-1111-111111111111"
        val resourceGroup = updateResourceGroup(groupObjectId = groupId)

        every { configGroup.allowGroupUpdate } returns true
        every { entraGroupMapper.expectedFromResourceGroup(resourceGroup) } returns
            EntraGroup(
                objectId = groupId,
                displayName = "UpdatedGroup",
                resourceGroupID = 12345,
            )
        every { entraGroupCommandService.updateGroup(resourceGroup) } returns
            EntraGroupCommandService.EntraGroupCommandResult(
                success = false,
                groupId = groupId,
                message = "Failed updating Entra group",
                error = RuntimeException("Graph error"),
            )

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            entraGroupCommandService.updateGroup(resourceGroup)
            groupProducerService.publishResourceGroupResponse(
                key = groupId,
                objectId = groupId,
                displayName = "UpdatedGroup",
                resourceGroupId = 12345L,
                traceId = traceId,
                status = EntraStatus.ERROR,
            )
        }

        verify(exactly = 0) {
            entraGroupStateService.storeAndPublish(any(), any(), any())
            entraGroupStateService.storeAndPublishIfChanged(any(), any(), any())
            entraGroupStateService.publish(any(), any(), any())
        }
    }

    @Test
    fun `process does not call graph when update has invalid resourceGroupId`() {
        val traceId = "trace-update-invalid-resource-id-123"
        val groupId = "11111111-1111-1111-1111-111111111111"
        val resourceGroup = updateResourceGroup(id = "abc", groupObjectId = groupId)

        every { configGroup.allowGroupUpdate } returns true

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            groupProducerService.publishResourceGroupResponse(
                key = groupId,
                objectId = groupId,
                displayName = "UpdatedGroup",
                resourceGroupId = null,
                traceId = traceId,
                status = EntraStatus.ERROR,
            )
        }

        verify(exactly = 0) {
            entraGroupMapper.expectedFromResourceGroup(any())
            entraGroupCommandService.updateGroup(any())
            entraGroupStateService.storeAndPublish(any(), any(), any())
            entraGroupStateService.storeAndPublishIfChanged(any(), any(), any())
        }
    }

    @Test
    fun `process does not call graph when create is missing resourceName`() {
        val resourceGroup = createResourceGroup(resourceName = null)
        val traceId = "trace-123"

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            groupProducerService.publishResourceGroupResponse(
                key = "12345",
                objectId = null,
                displayName = null,
                resourceGroupId = 12345L,
                traceId = traceId,
                status = EntraStatus.ERROR,
            )
        }

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
        val traceId = "trace-123"

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            groupProducerService.publishResourceGroupResponse(
                key = "abc",
                objectId = null,
                displayName = "TestGroup",
                resourceGroupId = null,
                traceId = traceId,
                status = EntraStatus.ERROR,
            )
        }

        verify(exactly = 0) {
            entraGroupStateService.findObjectIdByResourceGroupId(any())
            entraGroupCommandService.findGroupIdByResourceGroupId(any())
            entraGroupCommandService.createGroup(any())
            entraGroupStateService.storeAndPublishIfChanged(any(), any(), any())
        }
    }

    @Test
    fun `process does not call graph when create has groupObjectId`() {
        val traceId = "trace-123"
        val groupId = "11111111-1111-1111-1111-111111111111"
        val resourceGroup =
            createResourceGroup(
                groupObjectId = groupId,
            )

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            groupProducerService.publishResourceGroupResponse(
                key = groupId,
                objectId = groupId,
                displayName = "TestGroup",
                resourceGroupId = 12345L,
                traceId = traceId,
                status = EntraStatus.ERROR,
            )
        }

        verify(exactly = 0) {
            entraGroupStateService.findObjectIdByResourceGroupId(any())
            entraGroupCommandService.findGroupIdByResourceGroupId(any())
            entraGroupCommandService.createGroup(any())
            entraGroupStateService.storeAndPublishIfChanged(any(), any(), any())
        }
    }

    @Test
    fun `process deletes group when delete has verified groupObjectId and resourceGroupId`() {
        val traceId = "trace-delete-123"
        val groupId = "11111111-1111-1111-1111-111111111111"
        val resourceGroup = deleteResourceGroup(groupObjectId = groupId)

        every { configGroup.allowGroupDelete } returns true
        every {
            entraGroupCommandService.verifyGroupByIdAndResourceGroupId(
                groupId = groupId,
                resourceGroupId = "12345",
            )
        } returns
            EntraGroupCommandService.EntraGroupCommandResult(
                success = true,
                groupId = groupId,
                message = "Verified Entra group",
            )
        every {
            entraGroupCommandService.deleteGroupById(
                groupId = groupId,
                resourceGroupId = "12345",
            )
        } returns
            EntraGroupCommandService.EntraGroupCommandResult(
                success = true,
                groupId = groupId,
                message = "Deleted Entra group",
            )
        every {
            entraGroupStateService.deleteAndPublish(
                objectId = groupId,
                resourceGroupId = 12345L,
                traceId = traceId,
            )
        } returns true

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            entraGroupCommandService.verifyGroupByIdAndResourceGroupId(
                groupId = groupId,
                resourceGroupId = "12345",
            )
            entraGroupCommandService.deleteGroupById(
                groupId = groupId,
                resourceGroupId = "12345",
            )
            entraGroupStateService.deleteAndPublish(
                objectId = groupId,
                resourceGroupId = 12345L,
                traceId = traceId,
            )
        }
    }

    @Test
    fun `process does not delete when delete is missing groupObjectId`() {
        val traceId = "trace-delete-missing-object-123"
        val resourceGroup = deleteResourceGroup()

        every { configGroup.allowGroupDelete } returns true

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            groupProducerService.publishResourceGroupResponse(
                key = "12345",
                objectId = null,
                displayName = null,
                resourceGroupId = 12345L,
                traceId = traceId,
                status = EntraStatus.ERROR,
            )
        }

        verify(exactly = 0) {
            entraGroupCommandService.findGroupIdByResourceGroupId(any())
            entraGroupCommandService.verifyGroupByIdAndResourceGroupId(any(), any())
            entraGroupCommandService.deleteGroupById(any(), any())
            entraGroupStateService.findObjectIdByResourceGroupId(any())
            entraGroupStateService.deleteAndPublish(any(), any(), any())
        }
    }

    @Test
    fun `process does not delete when delete group verification fails`() {
        val traceId = "trace-delete-mismatch-123"
        val groupId = "11111111-1111-1111-1111-111111111111"
        val resourceGroup = deleteResourceGroup(groupObjectId = groupId)

        every { configGroup.allowGroupDelete } returns true
        every {
            entraGroupCommandService.verifyGroupByIdAndResourceGroupId(
                groupId = groupId,
                resourceGroupId = "12345",
            )
        } returns
            EntraGroupCommandService.EntraGroupCommandResult(
                success = false,
                groupId = groupId,
                message = "Entra group did not match resourceGroupId",
            )

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            entraGroupCommandService.verifyGroupByIdAndResourceGroupId(
                groupId = groupId,
                resourceGroupId = "12345",
            )
            groupProducerService.publishResourceGroupResponse(
                key = groupId,
                objectId = groupId,
                displayName = null,
                resourceGroupId = 12345L,
                traceId = traceId,
                status = EntraStatus.FAILED,
            )
        }

        verify(exactly = 0) {
            entraGroupCommandService.findGroupIdByResourceGroupId(any())
            entraGroupCommandService.deleteGroupById(any(), any())
            entraGroupStateService.findObjectIdByResourceGroupId(any())
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
            resourceId = id,
            resourceName = resourceName,
            idpGroupObjectId = groupObjectId,
        )

    private fun deleteResourceGroup(groupObjectId: String? = null): ResourceGroup =
        ResourceGroup(
            operation = ResourceGroupOperation.DELETE,
            resourceId = "12345",
            resourceName = null,
            idpGroupObjectId = groupObjectId,
        )

    private fun updateResourceGroup(
        id: String = "12345",
        groupObjectId: String,
    ): ResourceGroup =
        ResourceGroup(
            operation = ResourceGroupOperation.UPDATE,
            resourceId = id,
            resourceName = "UpdatedGroup",
            idpGroupObjectId = groupObjectId,
        )
}
