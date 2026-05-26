package no.novari.msgraphgateway.membership.device

data class DeviceResourceGroupMembership(
    val operation: OperationType,
    val entraGroupRef: String,
    val entraDeviceRef: String,
)
