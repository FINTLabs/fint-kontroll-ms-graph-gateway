package no.novari.msgraphgateway.membership.user

import no.novari.msgraphgateway.kafka.OperationType

data class UserResourceGroupMembership(
    val operation: OperationType,
    val entraGroupRef: String,
    val userGroupRef: String,
)
