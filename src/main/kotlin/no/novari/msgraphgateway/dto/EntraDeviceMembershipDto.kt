package no.novari.msgraphgateway.dto

import no.novari.msgraphgateway.entra.EntraStatus

data class EntraDeviceMembershipDto(
    val code: EntraStatus,
    val entraGroupRef: String,
    val entraDeviceRef: String,
)
