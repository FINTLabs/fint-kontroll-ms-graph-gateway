package no.novari.msgraphgateway.kafka.group

import no.novari.msgraphgateway.config.ConfigGroup
import no.novari.msgraphgateway.entra.EntraStatus
import no.novari.msgraphgateway.group.EntraGroupMapper
import no.novari.msgraphgateway.services.group.EntraGroupCommandService
import no.novari.msgraphgateway.services.group.EntraGroupStateService
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.UUID

@Service
class ResourceGroupConsumerService(
    private val configGroup: ConfigGroup,
    private val entraGroupMapper: EntraGroupMapper,
    private val entraGroupStateService: EntraGroupStateService,
    private val entraGroupCommandService: EntraGroupCommandService,
) {
    fun process(
        resourceGroup: ResourceGroup?,
        traceId: String,
    ) {
        val resolvedTraceId = traceId.takeUnless { it.isBlank() } ?: UUID.randomUUID().toString()

        log.debug(
            "Received resource-group command traceId={}, operation={}, id={}",
            resolvedTraceId,
            resourceGroup?.operation,
            resourceGroup?.id,
        )

        if (resourceGroup == null) {
            log.warn("ResourceGroup payload was null. traceId={}", resolvedTraceId)
            return
        }

        when (resourceGroup.operation) {
            ResourceGroupOperation.CREATE -> createAndStore(resourceGroup, resolvedTraceId)
            ResourceGroupOperation.UPDATE -> updateAndStore(resourceGroup, resolvedTraceId)
            ResourceGroupOperation.DELETE -> deleteAndPublish(resourceGroup, resolvedTraceId)
        }
    }

    private fun createAndStore(
        resourceGroup: ResourceGroup,
        traceId: String,
    ) {
        if (resourceGroup.id.toLongOrNull() == null) {
            log.warn(
                "Cannot create Entra group; resourceGroup.id is required and must be numeric. resourceGroupId={}, traceId={}",
                resourceGroup.id,
                traceId,
            )
            return
        }

        if (resourceGroup.resourceName.isNullOrBlank()) {
            log.warn(
                "Cannot create Entra group for ResourceGroupId {}; resourceName is required. traceId={}",
                resourceGroup.id,
                traceId,
            )
            return
        }

        if (!resourceGroup.groupObjectId.isNullOrBlank()) {
            log.warn(
                "Cannot create Entra group for ResourceGroupId {}; groupObjectId must be empty for CREATE. groupObjectId={}, traceId={}",
                resourceGroup.id,
                resourceGroup.groupObjectId,
                traceId,
            )
            return
        }

        when (val existingGroup = findExistingGroupForCreate(resourceGroup.id, traceId)) {
            is ExistingGroupLookup.Found -> {
                val storedAndPublished =
                    storeExpectedAndPublish(
                        resourceGroup = resourceGroup.copy(groupObjectId = existingGroup.groupId),
                        traceId = traceId,
                        forcePublish = true,
                        status = EntraStatus.CREATED,
                    )

                if (storedAndPublished) {
                    log.info(
                        "ResourceGroupId {} already existed as Entra group {}; local state was published. traceId={}",
                        resourceGroup.id,
                        existingGroup.groupId,
                        traceId,
                    )
                } else {
                    log.info(
                        "ResourceGroupId {} already exists as Entra group {}; skipping create. traceId={}",
                        resourceGroup.id,
                        existingGroup.groupId,
                        traceId,
                    )
                }
                return
            }

            ExistingGroupLookup.LookupFailed -> {
                return
            }

            ExistingGroupLookup.NotFound -> {
                Unit
            }
        }

        log.debug(
            "Creating Entra group {} for ResourceGroupId {}. traceId={}",
            resourceGroup.resourceName,
            resourceGroup.id,
            traceId,
        )

        val result = entraGroupCommandService.createGroup(resourceGroup)

        if (!result.success) {
            log.error(
                "ResourceGroupId {} was not created in Entra: {}. traceId={}",
                resourceGroup.id,
                result.message,
                traceId,
                result.error,
            )
            return
        }

        val groupId = result.groupId

        if (groupId.isNullOrBlank()) {
            log.error(
                "ResourceGroupId {} was created in Entra, but Graph response did not contain group id. traceId={}",
                resourceGroup.id,
                traceId,
            )
            return
        }

        val storedAndPublished =
            storeExpectedAndPublish(
                resourceGroup = resourceGroup.copy(groupObjectId = groupId),
                traceId = traceId,
                forcePublish = true,
                status = EntraStatus.CREATED,
            )

        if (!storedAndPublished) {
            log.warn(
                "ResourceGroupId {} was created in Entra as {}, but local state was not published. traceId={}",
                resourceGroup.id,
                groupId,
                traceId,
            )
            return
        }

        log.info(
            "ResourceGroupId {} was created in Entra as {}. traceId={}",
            resourceGroup.id,
            groupId,
            traceId,
        )
    }

    private fun updateAndStore(
        resourceGroup: ResourceGroup,
        traceId: String,
    ) {
        if (configGroup.allowGroupUpdate != true) {
            log.warn(
                "ResourceGroupId {} was NOT updated because ms-graph.group.allow-group-update is not true. traceId={}",
                resourceGroup.id,
                traceId,
            )
            return
        }

        if (resourceGroup.resourceName.isNullOrBlank()) {
            log.warn(
                "Cannot update Entra group for ResourceGroupId {}; resourceName is required. traceId={}",
                resourceGroup.id,
                traceId,
            )
            return
        }

        val groupId = resourceGroup.groupObjectId

        if (groupId.isNullOrBlank()) {
            log.warn(
                "Cannot update Entra group for ResourceGroupId {}; groupObjectId is required for UPDATE. traceId={}",
                resourceGroup.id,
                traceId,
            )
            return
        }

        log.info(
            "Updating Entra group {} for ResourceGroupId {}. traceId={}",
            groupId,
            resourceGroup.id,
            traceId,
        )

        val result = entraGroupCommandService.updateGroup(resourceGroup)

        if (!result.success) {
            log.error(
                "ResourceGroupId {} was not updated in Entra: {}. traceId={}",
                resourceGroup.id,
                result.message,
                traceId,
            )

            val publishedFailed =
                publishExpected(
                    resourceGroup = resourceGroup,
                    traceId = traceId,
                    status = EntraStatus.ERROR,
                )

            if (!publishedFailed) {
                log.warn(
                    "ResourceGroupId {} was not updated in Entra, and FAILED status was not published. traceId={}",
                    resourceGroup.id,
                    traceId,
                )
            }
            return
        }

        val storedAndPublished =
            storeExpectedAndPublish(
                resourceGroup = resourceGroup,
                traceId = traceId,
                forcePublish = true,
                status = EntraStatus.UPDATED,
            )

        if (!storedAndPublished) {
            log.warn(
                "ResourceGroupId {} was updated in Entra group {}, but local state was not published. traceId={}",
                resourceGroup.id,
                groupId,
                traceId,
            )
            return
        }

        log.info(
            "ResourceGroupId {} was updated in Entra group {} and local state was published. traceId={}",
            resourceGroup.id,
            groupId,
            traceId,
        )
    }

    private fun deleteAndPublish(
        resourceGroup: ResourceGroup,
        traceId: String,
    ) {
        if (configGroup.allowGroupDelete != true) {
            log.warn(
                "ResourceGroupId {} was NOT deleted because ms-graph.group.allow-group-delete is not true. traceId={}",
                resourceGroup.id,
                traceId,
            )
            return
        }

        val groupId =
            resourceGroup.groupObjectId
                ?: when (val lookup = findGroupForDelete(resourceGroup.id, traceId)) {
                    is DeleteGroupLookup.Found -> {
                        lookup.groupId
                    }

                    is DeleteGroupLookup.AlreadyDeleted -> {
                        deleteLocalStateForAlreadyDeletedGroup(
                            resourceGroupId = resourceGroup.id,
                            groupId = lookup.localGroupId,
                            traceId = traceId,
                        )
                        return
                    }

                    DeleteGroupLookup.LookupFailed -> {
                        return
                    }
                }

        val result =
            entraGroupCommandService.deleteGroupById(
                groupId = groupId,
                resourceGroupId = resourceGroup.id,
            )

        if (!result.success) {
            log.error(
                "ResourceGroupId {} was not deleted from Entra: {}. groupId={}, traceId={}",
                resourceGroup.id,
                result.message,
                groupId,
                traceId,
                result.error,
            )
            return
        }

        val deletedAndPublished =
            entraGroupStateService.deleteAndPublish(
                objectId = groupId,
                resourceGroupId = resourceGroup.id.toLongOrNull(),
                traceId = traceId,
            )

        if (!deletedAndPublished) {
            log.warn(
                "Entra group {} was deleted, but local state deletion/publish failed. ResourceGroupId={}, traceId={}",
                groupId,
                resourceGroup.id,
                traceId,
            )
            return
        }

        log.info(
            "ResourceGroupId {} was deleted from Entra group {}. traceId={}",
            resourceGroup.id,
            groupId,
            traceId,
        )
    }

    private fun findGroupForDelete(
        resourceGroupId: String,
        traceId: String,
    ): DeleteGroupLookup =
        try {
            val groupId = entraGroupCommandService.findGroupIdByResourceGroupId(resourceGroupId)

            if (groupId.isNullOrBlank()) {
                val localGroupId = entraGroupStateService.findObjectIdByResourceGroupId(resourceGroupId)

                log.info(
                    "ResourceGroupId {} had no matching Entra group; treating as already deleted. traceId={}",
                    resourceGroupId,
                    traceId,
                )
                DeleteGroupLookup.AlreadyDeleted(localGroupId)
            } else {
                DeleteGroupLookup.Found(groupId)
            }
        } catch (e: IllegalStateException) {
            log.error(
                "Skipping delete for ResourceGroupId {} because multiple matching Entra groups were found. traceId={}",
                resourceGroupId,
                traceId,
                e,
            )
            DeleteGroupLookup.LookupFailed
        } catch (e: Exception) {
            log.error(
                "Failed looking up Entra group for ResourceGroupId {}; skipping delete. traceId={}",
                resourceGroupId,
                traceId,
                e,
            )
            DeleteGroupLookup.LookupFailed
        }

    private fun deleteLocalStateForAlreadyDeletedGroup(
        resourceGroupId: String,
        groupId: String?,
        traceId: String,
    ) {
        if (groupId.isNullOrBlank()) {
            log.info(
                "ResourceGroupId {} had no local group state to delete. traceId={}",
                resourceGroupId,
                traceId,
            )
            return
        }

        val deletedAndPublished =
            entraGroupStateService.deleteAndPublish(
                objectId = groupId,
                resourceGroupId = resourceGroupId.toLongOrNull(),
                traceId = traceId,
            )

        if (!deletedAndPublished) {
            log.warn(
                "ResourceGroupId {} was already deleted from Entra, but local state deletion/publish failed. groupId={}, traceId={}",
                resourceGroupId,
                groupId,
                traceId,
            )
            return
        }

        log.info(
            "ResourceGroupId {} was already deleted from Entra; removed local state for group {}. traceId={}",
            resourceGroupId,
            groupId,
            traceId,
        )
    }

    private fun findExistingGroupForCreate(
        resourceGroupId: String,
        traceId: String,
    ): ExistingGroupLookup {
        val localGroupId = entraGroupStateService.findObjectIdByResourceGroupId(resourceGroupId)

        if (!localGroupId.isNullOrBlank()) {
            log.debug(
                "ResourceGroupId {} exists in local state as Entra group {}; verifying in Entra before create. traceId={}",
                resourceGroupId,
                localGroupId,
                traceId,
            )
        }

        return try {
            val graphGroupId = entraGroupCommandService.findGroupIdByResourceGroupId(resourceGroupId)

            if (graphGroupId.isNullOrBlank()) {
                deleteStaleLocalStateBeforeCreate(
                    resourceGroupId = resourceGroupId,
                    localGroupId = localGroupId,
                    traceId = traceId,
                )
                ExistingGroupLookup.NotFound
            } else {
                if (!localGroupId.isNullOrBlank() && localGroupId != graphGroupId) {
                    deleteStaleLocalStateBeforeCreate(
                        resourceGroupId = resourceGroupId,
                        localGroupId = localGroupId,
                        traceId = traceId,
                    )
                }

                log.info(
                    "ResourceGroupId {} already exists in Entra as group {}; skipping create. traceId={}",
                    resourceGroupId,
                    graphGroupId,
                    traceId,
                )
                ExistingGroupLookup.Found(graphGroupId)
            }
        } catch (e: IllegalStateException) {
            log.error(
                "Skipping create for ResourceGroupId {} because multiple matching Entra groups were found. traceId={}",
                resourceGroupId,
                traceId,
                e,
            )
            ExistingGroupLookup.LookupFailed
        } catch (e: Exception) {
            log.error(
                "Failed looking up Entra group for ResourceGroupId {}; skipping create to avoid a duplicate. traceId={}",
                resourceGroupId,
                traceId,
                e,
            )
            ExistingGroupLookup.LookupFailed
        }
    }

    private fun deleteStaleLocalStateBeforeCreate(
        resourceGroupId: String,
        localGroupId: String?,
        traceId: String,
    ) {
        if (localGroupId.isNullOrBlank()) {
            return
        }

        val deletedAndPublished =
            entraGroupStateService.deleteAndPublish(
                objectId = localGroupId,
                resourceGroupId = resourceGroupId.toLongOrNull(),
                traceId = traceId,
            )

        if (!deletedAndPublished) {
            log.warn(
                "ResourceGroupId {} had stale local state for group {}, but it was not deleted before CREATE. traceId={}",
                resourceGroupId,
                localGroupId,
                traceId,
            )
            return
        }

        log.info(
            "ResourceGroupId {} had stale local state for group {}; deleted it before CREATE. traceId={}",
            resourceGroupId,
            localGroupId,
            traceId,
        )
    }

    private fun storeExpectedAndPublish(
        resourceGroup: ResourceGroup,
        traceId: String,
        forcePublish: Boolean = false,
        status: EntraStatus,
    ): Boolean {
        val expectedEntraGroup = entraGroupMapper.expectedFromResourceGroup(resourceGroup)

        if (expectedEntraGroup.objectId.isNullOrBlank()) {
            log.warn(
                "Cannot store expected Entra group for ResourceGroupId {}; missing objectId. traceId={}",
                resourceGroup.id,
                traceId,
            )
            return false
        }

        if (expectedEntraGroup.resourceGroupID == null) {
            log.warn(
                "Cannot store expected Entra group {}; invalid resourceGroupId {}. traceId={}",
                expectedEntraGroup.objectId,
                resourceGroup.id,
                traceId,
            )
            return false
        }

        return if (forcePublish) {
            entraGroupStateService.storeAndPublish(expectedEntraGroup, traceId, status)
        } else {
            entraGroupStateService.storeAndPublishIfChanged(expectedEntraGroup, traceId, status)
        }
    }

    private fun publishExpected(
        resourceGroup: ResourceGroup,
        traceId: String,
        status: EntraStatus,
    ): Boolean {
        val expectedEntraGroup = entraGroupMapper.expectedFromResourceGroup(resourceGroup)

        if (expectedEntraGroup.objectId.isNullOrBlank()) {
            log.warn(
                "Cannot publish expected Entra group for ResourceGroupId {}; missing objectId. traceId={}",
                resourceGroup.id,
                traceId,
            )
            return false
        }

        if (expectedEntraGroup.resourceGroupID == null) {
            log.warn(
                "Cannot publish expected Entra group {}; invalid resourceGroupId {}. traceId={}",
                expectedEntraGroup.objectId,
                resourceGroup.id,
                traceId,
            )
            return false
        }

        return entraGroupStateService.publish(expectedEntraGroup, traceId, status)
    }

    private sealed interface ExistingGroupLookup {
        data class Found(
            val groupId: String,
        ) : ExistingGroupLookup

        data object NotFound : ExistingGroupLookup

        data object LookupFailed : ExistingGroupLookup
    }

    private sealed interface DeleteGroupLookup {
        data class Found(
            val groupId: String,
        ) : DeleteGroupLookup

        data class AlreadyDeleted(
            val localGroupId: String?,
        ) : DeleteGroupLookup

        data object LookupFailed : DeleteGroupLookup
    }

    companion object {
        private val log = LoggerFactory.getLogger(ResourceGroupConsumerService::class.java)
    }
}
