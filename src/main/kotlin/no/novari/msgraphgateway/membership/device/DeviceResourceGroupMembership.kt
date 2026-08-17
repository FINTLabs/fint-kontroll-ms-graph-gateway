package no.novari.msgraphgateway.membership.device

import no.novari.msgraphgateway.kafka.OperationType

data class DeviceResourceGroupMembership(
    val operation: OperationType,
    val entraGroupRef: String,
    val entraDeviceRef: String,
)
