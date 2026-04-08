package no.novari.msgraphgateway.entra

import java.io.Serializable

data class EntraUserExternalPayload(
    val firstName: String? = null,
    val lastName: String? = null,
    val email: String? = null,
    val userName: String? = null,
    val userPrincipalName: String? = null,
    val accountEnabled: Boolean? = null,
    var mainOrganisationUnitName: String? = null,
    var mainOrganisationUnitId: String? = null,
) : Serializable
