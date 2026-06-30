package no.novari.msgraphgateway.kafka.group

import io.mockk.Runs
import io.mockk.every
import io.mockk.just
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
    private val groupProducerService = mockk<GroupProducerService>()

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
        every { groupProducerService.publishGroupStatus(any(), any()) } just Runs
    }

    @Test
    fun `process creates group stores state and publishes CREATED status`() {
        val traceId = "trace-123"
        val groupId = "11111111-1111-1111-1111-111111111111"
        val resourceGroup =
            ResourceGroup(
                operation = ResourceGroupOperation.CREATE,
                id = "12345",
                resourceName = "TestGroup",
                identityProviderGroupObjectId = null,
            )

        every {
            entraGroupCommandService.createGroup(resourceGroup)
        } returns
            EntraGroupCommandService.EntraGroupCommandResult(
                success = true,
                groupId = groupId,
                message = "Created Entra group",
            )

        every {
            entraGroupMapper.expectedFromResourceGroup(
                resourceGroup.copy(identityProviderGroupObjectId = groupId),
            )
        } returns
            EntraGroup(
                objectId = groupId,
                displayName = "TestGroup",
                resourceGroupID = 12345,
            )

        every { entraGroupStateService.storeAndPublishIfChanged(any()) } returns true

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            entraGroupCommandService.createGroup(resourceGroup)
        }

        verify(exactly = 1) {
            entraGroupStateService.storeAndPublishIfChanged(
                match {
                    it.objectId == groupId &&
                        it.resourceGroupID == 12345L
                },
            )
        }

        verify(exactly = 1) {
            groupProducerService.publishGroupStatus(
                traceId,
                match {
                    it.code == EntraStatus.CREATED &&
                        it.resourceGroupId == "12345" &&
                        it.entraGroupRef == groupId
                },
            )
        }
    }

    @Test
    fun `process publishes FAILED and does not store state when create fails`() {
        val traceId = "trace-123"
        val resourceGroup =
            ResourceGroup(
                operation = ResourceGroupOperation.CREATE,
                id = "12345",
                resourceName = "TestGroup",
                identityProviderGroupObjectId = null,
            )

        every {
            entraGroupCommandService.createGroup(resourceGroup)
        } returns
            EntraGroupCommandService.EntraGroupCommandResult(
                success = false,
                message = "Failed creating Entra group",
                error = RuntimeException("Graph error"),
            )

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            entraGroupCommandService.createGroup(resourceGroup)
        }

        verify(exactly = 0) {
            entraGroupStateService.storeAndPublishIfChanged(any())
        }

        verify(exactly = 1) {
            groupProducerService.publishGroupStatus(
                traceId,
                match {
                    it.code == EntraStatus.FAILED &&
                        it.resourceGroupId == "12345" &&
                        it.entraGroupRef == null
                },
            )
        }
    }

    @Test
    fun `process publishes ERROR and does not store state when create succeeds without group id`() {
        val traceId = "trace-123"
        val resourceGroup =
            ResourceGroup(
                operation = ResourceGroupOperation.CREATE,
                id = "12345",
                resourceName = "TestGroup",
                identityProviderGroupObjectId = null,
            )

        every {
            entraGroupCommandService.createGroup(resourceGroup)
        } returns
            EntraGroupCommandService.EntraGroupCommandResult(
                success = true,
                groupId = null,
                message = "Created Entra group",
            )

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            entraGroupCommandService.createGroup(resourceGroup)
        }

        verify(exactly = 0) {
            entraGroupStateService.storeAndPublishIfChanged(any())
        }

        verify(exactly = 1) {
            groupProducerService.publishGroupStatus(
                traceId,
                match {
                    it.code == EntraStatus.ERROR &&
                        it.resourceGroupId == "12345" &&
                        it.entraGroupRef == null
                },
            )
        }
    }

    @Test
    fun `process publishes ERROR and does not call graph when create is missing resourceName`() {
        val traceId = "trace-123"
        val resourceGroup =
            ResourceGroup(
                operation = ResourceGroupOperation.CREATE,
                id = "12345",
                resourceName = null,
                identityProviderGroupObjectId = null,
            )

        service.process(resourceGroup, traceId)

        verify(exactly = 0) {
            entraGroupCommandService.createGroup(any())
        }

        verify(exactly = 0) {
            entraGroupStateService.storeAndPublishIfChanged(any())
        }

        verify(exactly = 1) {
            groupProducerService.publishGroupStatus(
                traceId,
                match {
                    it.code == EntraStatus.ERROR &&
                        it.resourceGroupId == "12345" &&
                        it.message == "resourceName is required for CREATE"
                },
            )
        }
    }

    @Test
    fun `process publishes ERROR and does not call graph when create has identityProviderGroupObjectId`() {
        val traceId = "trace-123"
        val groupId = "11111111-1111-1111-1111-111111111111"
        val resourceGroup =
            ResourceGroup(
                operation = ResourceGroupOperation.CREATE,
                id = "12345",
                resourceName = "TestGroup",
                identityProviderGroupObjectId = groupId,
            )

        service.process(resourceGroup, traceId)

        verify(exactly = 0) {
            entraGroupCommandService.createGroup(any())
        }

        verify(exactly = 0) {
            entraGroupStateService.storeAndPublishIfChanged(any())
        }

        verify(exactly = 1) {
            groupProducerService.publishGroupStatus(
                traceId,
                match {
                    it.code == EntraStatus.ERROR &&
                        it.resourceGroupId == "12345" &&
                        it.entraGroupRef == groupId &&
                        it.message == "identityProviderGroupObjectId must be empty for CREATE"
                },
            )
        }
    }

    @Test
    fun `process updates group stores state and publishes UPDATED status`() {
        val traceId = "trace-456"
        val groupId = "11111111-1111-1111-1111-111111111111"
        val resourceGroup =
            ResourceGroup(
                operation = ResourceGroupOperation.UPDATE,
                id = "12345",
                resourceName = "UpdatedGroup",
                identityProviderGroupObjectId = groupId,
            )

        every { entraGroupMapper.expectedFromResourceGroup(resourceGroup) } returns
            EntraGroup(
                objectId = groupId,
                displayName = "UpdatedGroup",
                resourceGroupID = 12345,
            )

        every {
            entraGroupCommandService.updateGroup(resourceGroup)
        } returns
            EntraGroupCommandService.EntraGroupCommandResult(
                success = true,
                groupId = groupId,
                message = "Updated Entra group",
            )

        every { entraGroupStateService.storeAndPublishIfChanged(any()) } returns true

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            entraGroupCommandService.updateGroup(resourceGroup)
        }

        verify(exactly = 1) {
            entraGroupStateService.storeAndPublishIfChanged(
                match {
                    it.objectId == groupId &&
                        it.resourceGroupID == 12345L
                },
            )
        }

        verify(exactly = 1) {
            groupProducerService.publishGroupStatus(
                traceId,
                match {
                    it.code == EntraStatus.UPDATED &&
                        it.resourceGroupId == "12345" &&
                        it.entraGroupRef == groupId
                },
            )
        }
    }

    @Test
    fun `process publishes FAILED and does not store state when update fails`() {
        val traceId = "trace-456"
        val groupId = "11111111-1111-1111-1111-111111111111"
        val resourceGroup =
            ResourceGroup(
                operation = ResourceGroupOperation.UPDATE,
                id = "12345",
                resourceName = "UpdatedGroup",
                identityProviderGroupObjectId = groupId,
            )

        every {
            entraGroupCommandService.updateGroup(resourceGroup)
        } returns
            EntraGroupCommandService.EntraGroupCommandResult(
                success = false,
                groupId = groupId,
                message = "Failed updating Entra group",
                error = RuntimeException("Graph error"),
            )

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            entraGroupCommandService.updateGroup(resourceGroup)
        }

        verify(exactly = 0) {
            entraGroupStateService.storeAndPublishIfChanged(any())
        }

        verify(exactly = 1) {
            groupProducerService.publishGroupStatus(
                traceId,
                match {
                    it.code == EntraStatus.FAILED &&
                        it.resourceGroupId == "12345" &&
                        it.entraGroupRef == groupId
                },
            )
        }
    }

    @Test
    fun `process publishes ERROR and does not call graph when update is missing resourceName`() {
        val traceId = "trace-456"
        val groupId = "11111111-1111-1111-1111-111111111111"
        val resourceGroup =
            ResourceGroup(
                operation = ResourceGroupOperation.UPDATE,
                id = "12345",
                resourceName = null,
                identityProviderGroupObjectId = groupId,
            )

        service.process(resourceGroup, traceId)

        verify(exactly = 0) {
            entraGroupCommandService.updateGroup(any())
        }

        verify(exactly = 0) {
            entraGroupStateService.storeAndPublishIfChanged(any())
        }

        verify(exactly = 1) {
            groupProducerService.publishGroupStatus(
                traceId,
                match {
                    it.code == EntraStatus.ERROR &&
                        it.resourceGroupId == "12345" &&
                        it.entraGroupRef == groupId &&
                        it.message == "resourceName is required for UPDATE"
                },
            )
        }
    }

    @Test
    fun `process publishes ERROR and does not call graph when update is missing identityProviderGroupObjectId`() {
        val traceId = "trace-456"
        val resourceGroup =
            ResourceGroup(
                operation = ResourceGroupOperation.UPDATE,
                id = "12345",
                resourceName = "UpdatedGroup",
                identityProviderGroupObjectId = null,
            )

        service.process(resourceGroup, traceId)

        verify(exactly = 0) {
            entraGroupCommandService.updateGroup(any())
        }

        verify(exactly = 0) {
            entraGroupStateService.storeAndPublishIfChanged(any())
        }

        verify(exactly = 1) {
            groupProducerService.publishGroupStatus(
                traceId,
                match {
                    it.code == EntraStatus.ERROR &&
                        it.resourceGroupId == "12345" &&
                        it.entraGroupRef == null &&
                        it.message == "identityProviderGroupObjectId is required for UPDATE"
                },
            )
        }
    }

    @Test
    fun `process deletes group deletes state and publishes DELETED status`() {
        val traceId = "trace-789"
        val groupId = "11111111-1111-1111-1111-111111111111"
        val resourceGroup =
            ResourceGroup(
                operation = ResourceGroupOperation.DELETE,
                id = "12345",
                identityProviderGroupObjectId = groupId,
                resourceName = null,
            )

        every { configGroup.allowGroupDelete } returns true
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

        every { entraGroupStateService.deleteAndPublish(groupId) } returns true

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            entraGroupCommandService.deleteGroupById(
                groupId = groupId,
                resourceGroupId = "12345",
            )
        }

        verify(exactly = 1) {
            entraGroupStateService.deleteAndPublish(groupId)
        }

        verify(exactly = 1) {
            groupProducerService.publishGroupStatus(
                traceId,
                match {
                    it.code == EntraStatus.DELETED &&
                        it.resourceGroupId == "12345" &&
                        it.entraGroupRef == groupId
                },
            )
        }
    }

    @Test
    fun `process publishes FAILED and does not delete state when delete fails`() {
        val traceId = "trace-789"
        val groupId = "11111111-1111-1111-1111-111111111111"
        val resourceGroup =
            ResourceGroup(
                operation = ResourceGroupOperation.DELETE,
                id = "12345",
                identityProviderGroupObjectId = groupId,
                resourceName = null,
            )

        every { configGroup.allowGroupDelete } returns true
        every {
            entraGroupCommandService.deleteGroupById(
                groupId = groupId,
                resourceGroupId = "12345",
            )
        } returns
            EntraGroupCommandService.EntraGroupCommandResult(
                success = false,
                groupId = groupId,
                message = "Failed deleting Entra group",
                error = RuntimeException("Graph error"),
            )

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            entraGroupCommandService.deleteGroupById(
                groupId = groupId,
                resourceGroupId = "12345",
            )
        }

        verify(exactly = 0) {
            entraGroupStateService.deleteAndPublish(any())
        }

        verify(exactly = 1) {
            groupProducerService.publishGroupStatus(
                traceId,
                match {
                    it.code == EntraStatus.FAILED &&
                        it.resourceGroupId == "12345" &&
                        it.entraGroupRef == groupId
                },
            )
        }
    }

    @Test
    fun `process publishes ERROR and does not delete when group delete is not allowed`() {
        val traceId = "trace-789"
        val groupId = "11111111-1111-1111-1111-111111111111"
        val resourceGroup =
            ResourceGroup(
                operation = ResourceGroupOperation.DELETE,
                id = "12345",
                identityProviderGroupObjectId = groupId,
                resourceName = null,
            )

        every { configGroup.allowGroupDelete } returns false

        service.process(resourceGroup, traceId)

        verify(exactly = 0) {
            entraGroupCommandService.findGroupIdByResourceGroupId(any())
        }

        verify(exactly = 0) {
            entraGroupCommandService.deleteGroupById(any(), any())
        }

        verify(exactly = 0) {
            entraGroupStateService.deleteAndPublish(any())
        }

        verify(exactly = 1) {
            groupProducerService.publishGroupStatus(
                traceId,
                match {
                    it.code == EntraStatus.ERROR &&
                        it.resourceGroupId == "12345" &&
                        it.entraGroupRef == groupId
                },
            )
        }
    }

    @Test
    fun `process publishes NO_CHANGES and does not delete when group is not found`() {
        val traceId = "trace-789"
        val resourceGroup =
            ResourceGroup(
                operation = ResourceGroupOperation.DELETE,
                id = "12345",
                identityProviderGroupObjectId = null,
                resourceName = null,
            )

        every { configGroup.allowGroupDelete } returns true
        every { entraGroupCommandService.findGroupIdByResourceGroupId("12345") } returns null

        service.process(resourceGroup, traceId)

        verify(exactly = 1) {
            entraGroupCommandService.findGroupIdByResourceGroupId("12345")
        }

        verify(exactly = 0) {
            entraGroupCommandService.deleteGroupById(any(), any())
        }

        verify(exactly = 0) {
            entraGroupStateService.deleteAndPublish(any())
        }

        verify(exactly = 1) {
            groupProducerService.publishGroupStatus(
                traceId,
                match {
                    it.code == EntraStatus.NO_CHANGES &&
                        it.resourceGroupId == "12345" &&
                        it.entraGroupRef == null
                },
            )
        }
    }

    @Test
    fun `process publishes ERROR when payload is null`() {
        val traceId = "trace-null"

        service.process(null, traceId)

        verify(exactly = 0) {
            entraGroupCommandService.createGroup(any())
        }

        verify(exactly = 0) {
            entraGroupCommandService.updateGroup(any())
        }

        verify(exactly = 0) {
            entraGroupCommandService.deleteGroupById(any(), any())
        }

        verify(exactly = 0) {
            entraGroupStateService.storeAndPublishIfChanged(any())
        }

        verify(exactly = 0) {
            entraGroupStateService.deleteAndPublish(any())
        }

        verify(exactly = 1) {
            groupProducerService.publishGroupStatus(
                traceId,
                match {
                    it.code == EntraStatus.ERROR &&
                        it.resourceGroupId == null &&
                        it.entraGroupRef == null
                },
            )
        }
    }
}
