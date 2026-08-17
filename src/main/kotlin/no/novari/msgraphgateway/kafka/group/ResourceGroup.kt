package no.novari.msgraphgateway.kafka.group

data class ResourceGroup(
    val operation: ResourceGroupOperation,
    val resourceId: String,
    val idpGroupObjectId: String? = null,
    val resourceName: String? = null,
)
