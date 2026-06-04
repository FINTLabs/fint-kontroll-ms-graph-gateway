package no.novari.msgraphgateway.entra

data class EntraGroupPayload(
    val objectId: String?,
    val displayName: String?,
    val resourceGroupId: Long? = null,
)
