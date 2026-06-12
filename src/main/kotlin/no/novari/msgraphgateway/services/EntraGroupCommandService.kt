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

        graphServiceClient
            .groups()
            .post(group)

        log.info("Created Entra group for ResourceGroupId {}", resourceGroup.id)
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

        graphServiceClient
            .groups()
            .byGroupId(groupId)
            .patch(group)

        log.info("Updated Entra group {} for ResourceGroupId {}", groupId, resourceGroup.id)
    }

    fun deleteGroup(resourceGroupId: String) {
        val groupId = findGroupIdByResourceGroupId(resourceGroupId)

        if (groupId.isNullOrBlank()) {
            log.warn("Could not find Entra group for ResourceGroupId {}; skipping delete", resourceGroupId)
            return
        }

        graphServiceClient
            .groups()
            .byGroupId(groupId)
            .delete()

        log.info("Deleted Entra group {} for ResourceGroupId {}", groupId, resourceGroupId)
    }

    fun findGroupIdByResourceGroupId(resourceGroupId: String?): String? {
        if (resourceGroupId.isNullOrBlank()) return null

        val attr = configGroup.resourceGroupIdAttribute ?: return null

        val groups =
            graphServiceClient
                .groups()
                .get { req ->
                    req.queryParameters?.select = arrayOf("id", "displayName", attr)
                }?.value
                ?: return null

        return groups
            .firstOrNull { group ->
                group.additionalData[attr]?.toString() == resourceGroupId
            }?.id
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
