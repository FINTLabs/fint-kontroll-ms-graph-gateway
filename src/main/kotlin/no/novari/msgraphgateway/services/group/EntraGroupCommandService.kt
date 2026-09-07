package no.novari.msgraphgateway.services.group

import com.microsoft.graph.models.Group
import com.microsoft.graph.serviceclient.GraphServiceClient
import com.microsoft.kiota.ApiException
import no.novari.msgraphgateway.config.ConfigGroup
import no.novari.msgraphgateway.entra.EntraStatus
import no.novari.msgraphgateway.group.EntraGroupMapper
import no.novari.msgraphgateway.kafka.group.ResourceGroup
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class EntraGroupCommandService(
    private val graphServiceClient: GraphServiceClient,
    private val configGroup: ConfigGroup,
    private val entraGroupMapper: EntraGroupMapper,
) {
    data class EntraGroupCommandResult(
        val success: Boolean,
        val groupId: String? = null,
        val message: String? = null,
        val error: Throwable? = null,
        val failureStatus: EntraStatus = EntraStatus.ERROR,
    )

    fun createGroup(resourceGroup: ResourceGroup): EntraGroupCommandResult {
        val group =
            Group().apply {
                displayName = entraGroupMapper.buildDisplayName(resourceGroup)
                mailEnabled = false
                securityEnabled = true
                mailNickname = entraGroupMapper.buildMailNickname(resourceGroup)
                uniqueName = resourceGroup.resourceId

                configGroup.resourceGroupIdAttribute
                    ?.takeIf { it.isNotBlank() }
                    ?.let { attr ->
                        additionalData[attr] = resourceGroup.resourceId
                    }
            }

        return runCatching {
            graphServiceClient
                .groups()
                .post(group)
        }.fold(
            onSuccess = { createdGroup ->
                log.debug(
                    "Created Entra group {} for ResourceGroupId {}",
                    createdGroup?.id,
                    resourceGroup.resourceId,
                )

                EntraGroupCommandResult(
                    success = true,
                    groupId = createdGroup?.id,
                    message = "Created Entra group",
                )
            },
            onFailure = {
                log.error(
                    "Failed creating Entra group for ResourceGroupId {}",
                    resourceGroup.resourceId,
                    it,
                )

                EntraGroupCommandResult(
                    success = false,
                    message = "Failed creating Entra group",
                    error = it,
                    failureStatus = classifyFailure(it),
                )
            },
        )
    }

    fun updateGroup(resourceGroup: ResourceGroup): EntraGroupCommandResult {
        val groupId = resourceGroup.idpGroupObjectId

        if (groupId.isNullOrBlank()) {
            log.warn(
                "Cannot update group for ResourceGroupId {}; missing identityProviderGroupObjectId",
                resourceGroup.resourceId,
            )

            return EntraGroupCommandResult(
                success = false,
                message = "Missing identityProviderGroupObjectId",
            )
        }

        val group =
            Group().apply {
                displayName = entraGroupMapper.buildDisplayName(resourceGroup)

                configGroup.resourceGroupIdAttribute
                    ?.takeIf { it.isNotBlank() }
                    ?.let { attr ->
                        additionalData[attr] = resourceGroup.resourceId
                    }
            }

        return runCatching {
            graphServiceClient
                .groups()
                .byGroupId(groupId)
                .patch(group)
        }.fold(
            onSuccess = {
                log.info(
                    "Updated Entra group {} for ResourceGroupId {}",
                    groupId,
                    resourceGroup.resourceId,
                )

                EntraGroupCommandResult(
                    success = true,
                    groupId = groupId,
                    message = "Updated Entra group",
                )
            },
            onFailure = {
                log.error(
                    "Failed updating Entra group {} for ResourceGroupId {}. Error {}",
                    groupId,
                    resourceGroup.resourceId,
                    it.message,
                )

                EntraGroupCommandResult(
                    success = false,
                    groupId = groupId,
                    message = "Failed updating Entra group",
                    error = it,
                    failureStatus = classifyFailure(it),
                )
            },
        )
    }

    fun deleteGroup(resourceGroupId: String): EntraGroupCommandResult {
        val groupId =
            try {
                findGroupIdByResourceGroupId(resourceGroupId)
            } catch (e: IllegalStateException) {
                log.error(
                    "Skipping delete for ResourceGroupId {} because multiple matching Entra groups were found",
                    resourceGroupId,
                    e,
                )

                return EntraGroupCommandResult(
                    success = false,
                    message = "Multiple matching Entra groups were found",
                    error = e,
                )
            } catch (e: Exception) {
                log.error(
                    "Failed looking up Entra group for ResourceGroupId {}; skipping delete",
                    resourceGroupId,
                    e,
                )

                return EntraGroupCommandResult(
                    success = false,
                    message = "Failed looking up Entra group",
                    error = e,
                    failureStatus = classifyFailure(e),
                )
            }

        if (groupId.isNullOrBlank()) {
            log.info(
                "Could not find Entra group for ResourceGroupId {}; treating as already deleted",
                resourceGroupId,
            )

            return EntraGroupCommandResult(
                success = true,
                groupId = null,
                message = "No matching Entra group found",
            )
        }

        return deleteGroupById(
            groupId = groupId,
            resourceGroupId = resourceGroupId,
        )
    }

    fun deleteGroupById(
        groupId: String,
        resourceGroupId: String,
    ): EntraGroupCommandResult =
        runCatching {
            graphServiceClient
                .groups()
                .byGroupId(groupId)
                .delete()
        }.fold(
            onSuccess = {
                log.info(
                    "Deleted Entra group {} for ResourceGroupId {}",
                    groupId,
                    resourceGroupId,
                )

                EntraGroupCommandResult(
                    success = true,
                    groupId = groupId,
                    message = "Deleted Entra group",
                )
            },
            onFailure = {
                log.error(
                    "Failed deleting Entra group {} for ResourceGroupId {}",
                    groupId,
                    resourceGroupId,
                    it,
                )

                EntraGroupCommandResult(
                    success = false,
                    groupId = groupId,
                    message = "Failed deleting Entra group",
                    error = it,
                    failureStatus = classifyFailure(it),
                )
            },
        )

    fun verifyGroupByIdAndResourceGroupId(
        groupId: String?,
        resourceGroupId: String?,
    ): EntraGroupCommandResult {
        if (groupId.isNullOrBlank()) {
            log.warn("Cannot verify group for delete; missing identityProviderGroupObjectId")

            return EntraGroupCommandResult(
                success = false,
                message = "Missing identityProviderGroupObjectId",
            )
        }

        if (resourceGroupId.isNullOrBlank()) {
            log.warn("Cannot verify group {}; missing resourceGroupId", groupId)

            return EntraGroupCommandResult(
                success = false,
                groupId = groupId,
                message = "Missing resourceGroupId",
            )
        }

        val attr =
            configGroup.resourceGroupIdAttribute
                ?.takeIf { it.isNotBlank() }
                ?: run {
                    log.warn("Cannot verify group {}; missing resourceGroupIdAttribute configuration", groupId)

                    return EntraGroupCommandResult(
                        success = false,
                        groupId = groupId,
                        message = "Missing resourceGroupIdAttribute configuration",
                    )
                }

        return runCatching {
            graphServiceClient
                .groups()
                .byGroupId(groupId)
                .get { req ->
                    req.queryParameters?.select = arrayOf("id", "displayName", attr)
                }
        }.fold(
            onSuccess = { group ->
                if (group == null) {
                    log.info("Could not find Entra group {} while verifying delete", groupId)

                    return EntraGroupCommandResult(
                        success = false,
                        groupId = groupId,
                        message = "No matching Entra group found",
                    )
                }

                val actualGroupId = group.id
                val actualResourceGroupId = group.additionalData[attr]?.toString()

                if (actualGroupId == groupId && actualResourceGroupId == resourceGroupId) {
                    log.debug(
                        "Verified Entra group {} for ResourceGroupId {}",
                        groupId,
                        resourceGroupId,
                    )

                    EntraGroupCommandResult(
                        success = true,
                        groupId = groupId,
                        message = "Verified Entra group",
                    )
                } else {
                    log.warn(
                        "Entra group {} did not match delete request. actualObjectId={}, expectedResourceGroupId={}, actualResourceGroupId={}",
                        groupId,
                        actualGroupId,
                        resourceGroupId,
                        actualResourceGroupId,
                    )

                    EntraGroupCommandResult(
                        success = false,
                        groupId = groupId,
                        message = "Entra group did not match resourceGroupId",
                    )
                }
            },
            onFailure = {
                if (it is ApiException && it.responseStatusCode == 404) {
                    log.info("Could not find Entra group {} while verifying delete", groupId)

                    EntraGroupCommandResult(
                        success = false,
                        groupId = groupId,
                        message = "No matching Entra group found",
                    )
                } else {
                    log.error(
                        "Failed verifying Entra group {} for ResourceGroupId {}; skipping delete",
                        groupId,
                        resourceGroupId,
                        it,
                    )

                    EntraGroupCommandResult(
                        success = false,
                        groupId = groupId,
                        message = "Failed verifying Entra group",
                        error = it,
                        failureStatus = classifyFailure(it),
                    )
                }
            },
        )
    }

    fun findGroupIdByResourceGroupId(resourceGroupId: String?): String? {
        if (resourceGroupId.isNullOrBlank()) return null

        val attr =
            configGroup.resourceGroupIdAttribute
                ?.takeIf { it.isNotBlank() }
                ?: throw IllegalArgumentException("Missing resourceGroupIdAttribute configuration; cannot check for existing groups")

        val escapedResourceGroupId = resourceGroupId.replace("'", "''")
        val groups = mutableListOf<Group>()

        var response =
            graphServiceClient
                .groups()
                .get { req ->
                    req.queryParameters?.filter = "$attr eq '$escapedResourceGroupId'"
                    req.queryParameters?.select = arrayOf("id", "displayName", attr)
                }

        while (response != null) {
            groups += response.value.orEmpty()

            response =
                response.odataNextLink
                    ?.takeIf { it.isNotBlank() }
                    ?.let { nextLink ->
                        graphServiceClient
                            .groups()
                            .withUrl(nextLink)
                            .get()
                    }
        }

        return when {
            groups.isEmpty() -> {
                null
            }

            groups.size == 1 -> {
                groups.single().id
            }

            else -> {
                log.error(
                    "Found {} groups with same resourceID {}={}:\n{}",
                    groups.size,
                    attr,
                    resourceGroupId,
                    groups.joinToString("\n") {
                        "- ${it.displayName} (${it.id})"
                    },
                )

                throw IllegalStateException(
                    "Found ${groups.size} groups with $attr=$resourceGroupId. Cannot determine which group to use.",
                )
            }
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(EntraGroupCommandService::class.java)

        private fun classifyFailure(error: Throwable): EntraStatus {
            val statusCode = (error as? ApiException)?.responseStatusCode

            return when {
                statusCode == null -> EntraStatus.FAILED
                statusCode == 408 || statusCode == 429 -> EntraStatus.FAILED
                statusCode in 500..599 -> EntraStatus.FAILED
                else -> EntraStatus.ERROR
            }
        }
    }
}
