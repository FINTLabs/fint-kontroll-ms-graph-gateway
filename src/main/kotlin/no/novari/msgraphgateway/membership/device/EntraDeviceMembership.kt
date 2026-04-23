package no.novari.msgraphgateway.membership.device

data class EntraDeviceMembership(
    val code: EntraStatus,
    val entraResourceRef: String,
    val entraDeviceRef: String,
)
