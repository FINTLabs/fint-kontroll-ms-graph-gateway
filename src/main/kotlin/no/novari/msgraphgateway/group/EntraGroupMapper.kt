package no.novari.msgraphgateway.group

import no.novari.msgraphgateway.config.ConfigGroup
import no.novari.msgraphgateway.entra.group.EntraGroup
import no.novari.msgraphgateway.kafka.group.ResourceGroup
import org.springframework.stereotype.Component

@Component
class EntraGroupMapper(
    private val configGroup: ConfigGroup,
) {
    fun expectedFromResourceGroup(resourceGroup: ResourceGroup): EntraGroup =
        EntraGroup(
            objectId = resourceGroup.groupObjectId,
            displayName = buildDisplayName(resourceGroup),
            resourceGroupID = resourceGroup.id.toLongOrNull(),
        )

    fun buildDisplayName(resourceGroup: ResourceGroup): String {
        val prefix = configGroup.prefix.orEmpty()
        val suffix = configGroup.suffix.orEmpty()
        return "$prefix${resourceGroup.resourceName}$suffix"
    }

    fun buildMailNickname(resourceGroup: ResourceGroup): String =
        buildDisplayName(resourceGroup)
            .lowercase()
            .replace(Regex("[^a-z0-9]"), "-")
            .trim('-')
            .ifBlank { "group-${resourceGroup.id}" }
}
