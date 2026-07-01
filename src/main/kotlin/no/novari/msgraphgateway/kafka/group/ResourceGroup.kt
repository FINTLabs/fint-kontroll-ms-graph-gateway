package no.novari.msgraphgateway.kafka.group

data class ResourceGroup(
    val operation: ResourceGroupOperation,
    val id: String,
    val groupObjectId: String? = null,
    val resourceName: String? = null,
)
