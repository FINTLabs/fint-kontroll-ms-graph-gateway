package no.novari.msgraphgateway.dto
import no.novari.msgraphgateway.entra.EntraStatus

data class EntraGroupStatusDto(
    val code: EntraStatus,
    val resourceGroupId: String?,
    val entraGroupRef: String?,
    val message: String? = null,
)
