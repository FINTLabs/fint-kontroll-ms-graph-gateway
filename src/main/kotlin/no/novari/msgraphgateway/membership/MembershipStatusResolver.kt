package no.novari.msgraphgateway.membership

import no.novari.msgraphgateway.entra.EntraStatus
import no.novari.msgraphgateway.kafka.OperationType

object MembershipStatusResolver {
    fun shouldSkipOperation(
        existingStatus: EntraStatus?,
        operation: OperationType,
    ): Boolean =
        when (operation) {
            OperationType.ADD -> existingStatus == EntraStatus.ADDED
            OperationType.REMOVE -> existingStatus == EntraStatus.REMOVED
        }

    fun persistedStatus(
        operation: OperationType,
        status: EntraStatus,
    ): EntraStatus {
        if (status != EntraStatus.NO_CHANGES) {
            return status
        }

        return when (operation) {
            OperationType.ADD -> EntraStatus.ADDED
            OperationType.REMOVE -> EntraStatus.REMOVED
        }
    }
}
