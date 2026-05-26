package no.novari.msgraphgateway.membership.device

data class EntraDeviceMembership(
    val code: EntraStatus,
    val entraGroupRef: String,
    val entraDeviceRef: String,
)
