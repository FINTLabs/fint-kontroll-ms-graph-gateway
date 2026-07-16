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
    private val groupProducerService: GroupProducerService,
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
            resourceGroup?.resourceId,
        )

        if (resourceGroup == null) {
            log.warn("ResourceGroup payload was null. traceId={}", resolvedTraceId)
            publishResourceGroupResponse(null, resolvedTraceId, EntraStatus.ERROR)
            return
        }

        try {
            when (resourceGroup.operation) {
                ResourceGroupOperation.CREATE -> createAndStore(resourceGroup, resolvedTraceId)
                ResourceGroupOperation.UPDATE -> updateAndStore(resourceGroup, resolvedTraceId)
                ResourceGroupOperation.DELETE -> deleteAndPublish(resourceGroup, resolvedTraceId)
            }
        } catch (e: Exception) {
            log.error(
                "Failed processing resource-group command. traceId={}, operation={}, resourceGroupId={}",
                resolvedTraceId,
                resourceGroup.operation,
                resourceGroup.resourceId,
                e,
            )
            publishResourceGroupResponse(resourceGroup, resolvedTraceId, EntraStatus.ERROR)
        }
    }

    private fun createAndStore(
        resourceGroup: ResourceGroup,
        traceId: String,
    ) {
        if (resourceGroup.resourceId.toLongOrNull() == null) {
            log.warn(
                "Cannot create Entra group; resourceGroup.id is required and must be numeric. resourceGroupId={}, traceId={}",
                resourceGroup.resourceId,
                traceId,
            )
            publishResourceGroupResponse(resourceGroup, traceId, EntraStatus.ERROR)
            return
        }

        if (resourceGroup.resourceName.isNullOrBlank()) {
            log.warn(
                "Cannot create Entra group for ResourceGroupId {}; resourceName is required. traceId={}",
                resourceGroup.resourceId,
                traceId,
            )
            publishResourceGroupResponse(resourceGroup, traceId, EntraStatus.ERROR)
            return
        }

        if (!resourceGroup.idpGroupObjectId.isNullOrBlank()) {
            log.warn(
                "Cannot create Entra group for ResourceGroupId {}; groupObjectId must be empty for CREATE. groupObjectId={}, traceId={}",
                resourceGroup.resourceId,
                resourceGroup.idpGroupObjectId,
                traceId,
            )
            publishResourceGroupResponse(resourceGroup, traceId, EntraStatus.ERROR)
            return
        }

        when (val existingGroup = findExistingGroupForCreate(resourceGroup.resourceId, traceId)) {
            is ExistingGroupLookup.Found -> {
                val storedAndPublished =
                    storeExpectedAndPublish(
                        resourceGroup = resourceGroup.copy(idpGroupObjectId = existingGroup.groupId),
                        traceId = traceId,
                        forcePublish = true,
                        status = EntraStatus.NO_CHANGES,
                    )

                if (storedAndPublished) {
                    log.info(
                        "ResourceGroupId {} already existed as Entra group {}; local state was published. traceId={}",
                        resourceGroup.resourceId,
                        existingGroup.groupId,
                        traceId,
                    )
                } else {
                    log.info(
                        "ResourceGroupId {} already exists as Entra group {}; skipping create. traceId={}",
                        resourceGroup.resourceId,
                        existingGroup.groupId,
                        traceId,
                    )
                    publishResourceGroupResponse(
                        resourceGroup = resourceGroup.copy(idpGroupObjectId = existingGroup.groupId),
                        traceId = traceId,
                        status = EntraStatus.NO_CHANGES,
                    )
                }
                return
            }

            ExistingGroupLookup.LookupFailed -> {
                publishResourceGroupResponse(resourceGroup, traceId, EntraStatus.FAILED)
                return
            }

            ExistingGroupLookup.NotFound -> {
                Unit
            }
        }

        log.debug(
            "Creating Entra group {} for ResourceGroupId {}. traceId={}",
            resourceGroup.resourceName,
            resourceGroup.resourceId,
            traceId,
        )

        val result = entraGroupCommandService.createGroup(resourceGroup)

        if (!result.success) {
            log.error(
                "ResourceGroupId {} was not created in Entra: {}. traceId={}",
                resourceGroup.resourceId,
                result.message,
                traceId,
                result.error,
            )
            publishResourceGroupResponse(resourceGroup, traceId, EntraStatus.ERROR)
            return
        }

        val groupId = result.groupId

        if (groupId.isNullOrBlank()) {
            log.error(
                "ResourceGroupId {} was created in Entra, but Graph response did not contain group id. traceId={}",
                resourceGroup.resourceId,
                traceId,
            )
            publishResourceGroupResponse(resourceGroup, traceId, EntraStatus.ERROR)
            return
        }

        val storedAndPublished =
            storeExpectedAndPublish(
                resourceGroup = resourceGroup.copy(idpGroupObjectId = groupId),
                traceId = traceId,
                forcePublish = true,
                status = EntraStatus.CREATED,
            )

        if (!storedAndPublished) {
            log.warn(
                "ResourceGroupId {} was created in Entra as {}, but local state was not published. traceId={}",
                resourceGroup.resourceId,
                groupId,
                traceId,
            )
            publishResourceGroupResponse(
                resourceGroup = resourceGroup.copy(idpGroupObjectId = groupId),
                traceId = traceId,
                status = EntraStatus.CREATED,
            )
            return
        }

        log.info(
            "ResourceGroupId {} was created in Entra as {}. traceId={}",
            resourceGroup.resourceId,
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
                resourceGroup.resourceId,
                traceId,
            )
            publishResourceGroupResponse(resourceGroup, traceId, EntraStatus.FAILED)
            return
        }

        val resourceGroupId = resourceGroup.resourceId.toLongOrNull()

        if (resourceGroupId == null) {
            log.warn(
                "Cannot update Entra group; resourceGroup.id is required and must be numeric. resourceGroupId={}, traceId={}",
                resourceGroup.resourceId,
                traceId,
            )
            publishResourceGroupResponse(resourceGroup, traceId, EntraStatus.ERROR)
            return
        }

        if (resourceGroup.resourceName.isNullOrBlank()) {
            log.warn(
                "Cannot update Entra group for ResourceGroupId {}; resourceName is required. traceId={}",
                resourceGroup.resourceId,
                traceId,
            )
            publishResourceGroupResponse(resourceGroup, traceId, EntraStatus.ERROR)
            return
        }

        val groupId = resourceGroup.idpGroupObjectId

        if (groupId.isNullOrBlank()) {
            log.warn(
                "Cannot update Entra group for ResourceGroupId {}; groupObjectId is required for UPDATE. traceId={}",
                resourceGroup.resourceId,
                traceId,
            )
            publishResourceGroupResponse(resourceGroup, traceId, EntraStatus.ERROR)
            return
        }

        if (hasResourceGroupIdConflict(resourceGroup, groupId, resourceGroupId, traceId)) {
            publishResourceGroupResponse(resourceGroup, traceId, EntraStatus.ERROR)
            return
        }

        val expectedEntraGroup = entraGroupMapper.expectedFromResourceGroup(resourceGroup)
        if (entraGroupStateService.isUnchanged(expectedEntraGroup)) {
            log.info(
                "ResourceGroupId {} was unchanged for Entra group {}. traceId={}",
                resourceGroup.resourceId,
                groupId,
                traceId,
            )
            publishResourceGroupResponse(resourceGroup, traceId, EntraStatus.NO_CHANGES)
            return
        }

        log.info(
            "Updating Entra group {} for ResourceGroupId {}. traceId={}",
            groupId,
            resourceGroup.resourceId,
            traceId,
        )

        val result = entraGroupCommandService.updateGroup(resourceGroup)

        if (!result.success) {
            log.error(
                "ResourceGroupId {} was not updated in Entra: {}. traceId={}",
                resourceGroup.resourceId,
                result.message,
                traceId,
            )

            val publishedError =
                publishResourceGroupResponse(resourceGroup, traceId, EntraStatus.ERROR)

            if (!publishedError) {
                log.warn(
                    "ResourceGroupId {} was not updated in Entra, and ERROR status was not published. traceId={}",
                    resourceGroup.resourceId,
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
                resourceGroup.resourceId,
                groupId,
                traceId,
            )
            publishResourceGroupResponse(resourceGroup, traceId, EntraStatus.UPDATED)
            return
        }

        log.info(
            "ResourceGroupId {} was updated in Entra group {} and local state was published. traceId={}",
            resourceGroup.resourceId,
            groupId,
            traceId,
        )
    }

    private fun hasResourceGroupIdConflict(
        resourceGroup: ResourceGroup,
        groupId: String,
        resourceGroupId: Long,
        traceId: String,
    ): Boolean {
        val storedObjectId = entraGroupStateService.findObjectIdByResourceGroupId(resourceGroup.resourceId)
        if (!storedObjectId.isNullOrBlank() && !storedObjectId.equals(groupId, ignoreCase = true)) {
            log.warn(
                "Cannot update Entra group {}; ResourceGroupId {} is already linked to Entra group {}. traceId={}",
                groupId,
                resourceGroup.resourceId,
                storedObjectId,
                traceId,
            )
            return true
        }

        val storedResourceGroupId = entraGroupStateService.findResourceGroupIdByObjectId(groupId)
        if (storedResourceGroupId != null && storedResourceGroupId != resourceGroupId) {
            log.warn(
                "Cannot update Entra group {}; objectId is already linked to ResourceGroupId {}, not {}. traceId={}",
                groupId,
                storedResourceGroupId,
                resourceGroup.resourceId,
                traceId,
            )
            return true
        }

        return false
    }

    private fun deleteAndPublish(
        resourceGroup: ResourceGroup,
        traceId: String,
    ) {
        if (configGroup.allowGroupDelete != true) {
            log.warn(
                "ResourceGroupId {} was NOT deleted because ms-graph.group.allow-group-delete is not true. traceId={}",
                resourceGroup.resourceId,
                traceId,
            )
            publishResourceGroupResponse(resourceGroup, traceId, EntraStatus.FAILED)
            return
        }

        val groupId = resourceGroup.idpGroupObjectId

        if (groupId.isNullOrBlank()) {
            log.warn(
                "Cannot delete Entra group for ResourceGroupId {}; groupObjectId is required for DELETE. traceId={}",
                resourceGroup.resourceId,
                traceId,
            )
            publishResourceGroupResponse(resourceGroup, traceId, EntraStatus.ERROR)
            return
        }

        val verifiedGroup =
            entraGroupCommandService.verifyGroupByIdAndResourceGroupId(
                groupId = groupId,
                resourceGroupId = resourceGroup.resourceId,
            )

        if (!verifiedGroup.success) {
            log.warn(
                "ResourceGroupId {} was NOT deleted because Entra group {} could not be verified: {}. traceId={}",
                resourceGroup.resourceId,
                groupId,
                verifiedGroup.message,
                traceId,
                verifiedGroup.error,
            )
            publishResourceGroupResponse(resourceGroup, traceId, EntraStatus.FAILED)
            return
        }

        val result =
            entraGroupCommandService.deleteGroupById(
                groupId = groupId,
                resourceGroupId = resourceGroup.resourceId,
            )

        if (!result.success) {
            log.error(
                "ResourceGroupId {} was not deleted from Entra: {}. groupId={}, traceId={}",
                resourceGroup.resourceId,
                result.message,
                groupId,
                traceId,
                result.error,
            )
            publishResourceGroupResponse(resourceGroup, traceId, EntraStatus.ERROR)
            return
        }

        val deletedAndPublished =
            entraGroupStateService.deleteAndPublish(
                objectId = groupId,
                resourceGroupId = resourceGroup.resourceId.toLongOrNull(),
                traceId = traceId,
            )

        if (!deletedAndPublished) {
            log.warn(
                "Entra group {} was deleted, but local state deletion/publish failed. ResourceGroupId={}, traceId={}",
                groupId,
                resourceGroup.resourceId,
                traceId,
            )
            publishResourceGroupResponse(resourceGroup, traceId, EntraStatus.DELETED)
            return
        }

        log.info(
            "ResourceGroupId {} was deleted from Entra group {}. traceId={}",
            resourceGroup.resourceId,
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
                resourceGroup.resourceId,
                traceId,
            )
            return false
        }

        if (expectedEntraGroup.resourceGroupID == null) {
            log.warn(
                "Cannot store expected Entra group {}; invalid resourceGroupId {}. traceId={}",
                expectedEntraGroup.objectId,
                resourceGroup.resourceId,
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

    private fun publishResourceGroupResponse(
        resourceGroup: ResourceGroup?,
        traceId: String,
        status: EntraStatus,
    ): Boolean {
        val objectId = resourceGroup?.idpGroupObjectId?.takeIf { it.isNotBlank() }
        val key =
            objectId
                ?: resourceGroup
                    ?.resourceId
                    ?.takeIf { it.isNotBlank() }
                ?: traceId

        val displayName =
            resourceGroup
                ?.takeIf { !it.resourceName.isNullOrBlank() }
                ?.let { group ->
                    runCatching {
                        entraGroupMapper.buildDisplayName(group)
                    }.getOrDefault(group.resourceName.orEmpty())
                }

        return runCatching {
            groupProducerService.publishResourceGroupResponse(
                key = key,
                objectId = objectId,
                displayName = displayName,
                resourceGroupId = resourceGroup?.resourceId?.toLongOrNull(),
                traceId = traceId,
                status = status,
            )
        }.onFailure {
            log.error(
                "Failed publishing resource-group response. key={}, traceId={}, status={}",
                key,
                traceId,
                status,
                it,
            )
        }.isSuccess
    }

    private sealed interface ExistingGroupLookup {
        data class Found(
            val groupId: String,
        ) : ExistingGroupLookup

        data object NotFound : ExistingGroupLookup

        data object LookupFailed : ExistingGroupLookup
    }

    companion object {
        private val log = LoggerFactory.getLogger(ResourceGroupConsumerService::class.java)
    }
}
