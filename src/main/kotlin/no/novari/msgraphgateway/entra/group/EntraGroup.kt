package no.novari.msgraphgateway.entra.group

import com.microsoft.graph.models.Group
import no.novari.msgraphgateway.config.ConfigGroup
import no.novari.msgraphgateway.entra.EntraStatus
import org.slf4j.LoggerFactory
import java.io.Serializable

data class EntraGroup(
    val objectId: String? = null,
    val displayName: String? = null,
    val resourceGroupID: Long? = null,
    val traceId: String? = null,
    val status: EntraStatus? = null,
) : Serializable {
    constructor(
        group: Group,
        configGroup: ConfigGroup,
        traceId: String? = null,
    ) : this(
        objectId = group.id,
        displayName = group.displayName,
        resourceGroupID = getResourceGroupId(group, configGroup),
        traceId = traceId,
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

    fun toPayload(status: EntraStatus? = this.status): EntraGroupPayload =
        EntraGroupPayload(
            objectId = objectId,
            displayName = displayName,
            resourceGroupId = resourceGroupID,
            traceId = traceId,
            status = status,
        )
}
