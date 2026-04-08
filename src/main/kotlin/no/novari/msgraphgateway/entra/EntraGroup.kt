package no.novari.msgraphgateway.entra

import com.microsoft.graph.models.Group
import no.novari.msgraphgateway.config.ConfigGroup
import org.slf4j.LoggerFactory
import java.io.Serializable

data class EntraGroup(
    val id: String? = null,
    val displayName: String? = null,
    val resourceGroupID: Long? = null,
) : Serializable {
    constructor(group: Group, configGroup: ConfigGroup) : this(
        id = group.id,
        displayName = group.displayName,
        resourceGroupID = getResourceGroupId(group, configGroup),
    )

    companion object {
        private val log = LoggerFactory.getLogger(EntraGroup::class.java)

        private fun getResourceGroupId(
            group: Group,
            configGroup: ConfigGroup,
        ): Long? {
            val key = configGroup.resourceGroupIdAttribute ?: return null
            val raw = group.additionalData[key] ?: return null

            return raw.toString().toLongOrNull() ?: run {
                log.warn("Error converting value {} to long", raw)
                throw NumberFormatException("Cannot convert '$raw' to Long")
            }
        }
    }
}
