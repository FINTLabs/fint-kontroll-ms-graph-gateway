package no.novari.msgraphgateway.controller

import no.novari.msgraphgateway.entra.EntraGroup
import no.novari.msgraphgateway.entra.EntraUser
import java.io.Serializable

data class UserWithGroupsDto(
    val user: EntraUser? = null,
    val groups: List<EntraGroup> = emptyList(),
) : Serializable
