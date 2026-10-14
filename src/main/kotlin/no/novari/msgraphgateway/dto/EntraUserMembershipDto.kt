package no.novari.msgraphgateway.dto

import no.novari.msgraphgateway.entra.EntraStatus

data class EntraUserMembershipDto(
    val code: EntraStatus,
    val entraGroupRef: String,
    val entraUserRef: String,
)
