package no.novari.msgraphgateway.entra

data class EntraUserPayload(
    val mail: String? = null,
    val userPrincipalName: String? = null,
    var employeeId: String? = null,
    var studentId: String? = null,
    val accountEnabled: Boolean? = null,
)
