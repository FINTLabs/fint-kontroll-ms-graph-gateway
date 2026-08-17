package no.novari.msgraphgateway.entra.group

import no.novari.msgraphgateway.entra.EntraStatus

data class EntraGroupPayload(
    val objectId: String?,
    val displayName: String?,
    val resourceGroupId: Long? = null,
    val traceId: String? = null,
    val status: EntraStatus? = null,
)
