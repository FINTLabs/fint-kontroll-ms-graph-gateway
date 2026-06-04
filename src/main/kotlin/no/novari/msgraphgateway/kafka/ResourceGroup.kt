package no.novari.msgraphgateway.kafka

data class ResourceGroup(
    val id: String? = null,
    val resourceId: String? = null,
    val displayName: String? = null,
    val identityProviderGroupObjectId: String? = null,
    val resourceName: String? = null,
    val resourceType: String? = null,
    val resourceLimit: String? = null,
)
