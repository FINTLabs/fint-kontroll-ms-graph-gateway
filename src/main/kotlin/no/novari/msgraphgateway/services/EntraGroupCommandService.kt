package no.novari.msgraphgateway.group

import com.microsoft.graph.models.Group
import com.microsoft.graph.serviceclient.GraphServiceClient
import no.novari.msgraphgateway.config.ConfigGroup
import no.novari.msgraphgateway.kafka.group.ResourceGroup
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class EntraGroupCommandService(
    private val graphServiceClient: GraphServiceClient,
    private val configGroup: ConfigGroup,
) {
    private enum class GroupLookupOperation {
        UPSERT,
        DELETE,
    }

    fun createGroup(resourceGroup: ResourceGroup) {
        val group =
            Group().apply {
                displayName = buildDisplayName(resourceGroup)
                mailEnabled = false
                securityEnabled = true
                mailNickname = buildMailNickname(resourceGroup)

                configGroup.resourceGroupIdAttribute
                    ?.takeIf { it.isNotBlank() }
                    ?.let { attr ->
                        additionalData[attr] = resourceGroup.id
                    }
            }

        runCatching {
            graphServiceClient
                .groups()
                .post(group)
        }.onSuccess {
            log.info(
                "Created Entra group for ResourceGroupId {}",
                resourceGroup.id,
            )
        }.onFailure {
            log.error(
                "Failed creating Entra group for ResourceGroupId {}",
                resourceGroup.id,
                it,
            )
        }
    }

    fun updateGroup(resourceGroup: ResourceGroup) {
        val groupId = resourceGroup.identityProviderGroupObjectId

        if (groupId.isNullOrBlank()) {
            log.warn(
                "Cannot update group for ResourceGroupId {}; missing identityProviderGroupObjectId",
                resourceGroup.id,
            )
            return
        }

        val group =
            Group().apply {
                displayName = buildDisplayName(resourceGroup)

                configGroup.resourceGroupIdAttribute
                    ?.takeIf { it.isNotBlank() }
                    ?.let { attr ->
                        additionalData[attr] = resourceGroup.id
                    }
            }

        runCatching {
            graphServiceClient
                .groups()
                .byGroupId(groupId)
                .patch(group)
        }.onSuccess {
            log.info(
                "Updated Entra group {} for ResourceGroupId {}",
                groupId,
                resourceGroup.id,
            )
        }.onFailure {
            log.error(
                "Failed updating Entra group {} for ResourceGroupId {}",
                groupId,
                resourceGroup.id,
                it,
            )
        }
    }

    fun deleteGroup(resourceGroupId: String) {
        val groupId =
            try {
                findGroupIdByResourceGroupId(resourceGroupId)
            } catch (e: IllegalStateException) {
                log.error(
                    "Skipping delete for ResourceGroupId {} because multiple matching Entra groups were found",
                    resourceGroupId,
                    e,
                )
                return
            } catch (e: Exception) {
                log.error(
                    "Failed looking up Entra group for ResourceGroupId {}; skipping delete",
                    resourceGroupId,
                    e,
                )
                return
            }

        if (groupId.isNullOrBlank()) {
            log.warn(
                "Could not find Entra group for ResourceGroupId {}; skipping delete",
                resourceGroupId,
            )
            return
        }

        runCatching {
            graphServiceClient
                .groups()
                .byGroupId(groupId)
                .delete()
        }.onSuccess {
            log.info(
                "Deleted Entra group {} for ResourceGroupId {}",
                groupId,
                resourceGroupId,
            )
        }.onFailure {
            log.error(
                "Failed deleting Entra group {} for ResourceGroupId {}",
                groupId,
                resourceGroupId,
                it,
            )
        }
    }

    fun findGroupIdByResourceGroupId(resourceGroupId: String?): String? {
        if (resourceGroupId.isNullOrBlank()) return null

        val attr =
            configGroup.resourceGroupIdAttribute
                ?.takeIf { it.isNotBlank() }
                ?: return null

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

    private fun buildDisplayName(resourceGroup: ResourceGroup): String {
        val prefix = configGroup.prefix.orEmpty()
        val suffix = configGroup.suffix.orEmpty()
        return "$prefix${resourceGroup.resourceName}$suffix"
    }

    private fun buildMailNickname(resourceGroup: ResourceGroup): String =
        buildDisplayName(resourceGroup)
            .lowercase()
            .replace(Regex("[^a-z0-9]"), "-")
            .trim('-')
            .ifBlank { "group-${resourceGroup.id}" }

    companion object {
        private val log = LoggerFactory.getLogger(EntraGroupCommandService::class.java)
    }
}
