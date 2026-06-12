package no.novari.msgraphgateway.entra.group

data class EntraGroupPayload(
    val objectId: String?,
    val displayName: String?,
    val resourceGroupId: Long? = null,
)
