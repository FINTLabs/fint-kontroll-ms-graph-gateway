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

    fun responseStatusForDuplicateOperation(operation: OperationType): EntraStatus =
        when (operation) {
            OperationType.ADD -> EntraStatus.ALREADY_PRESENT
            OperationType.REMOVE -> EntraStatus.ALREADY_REMOVED
        }

    fun statusToStore(responseStatus: EntraStatus): EntraStatus =
        when (responseStatus) {
            EntraStatus.ALREADY_PRESENT -> EntraStatus.ADDED
            EntraStatus.ALREADY_REMOVED -> EntraStatus.REMOVED
            else -> responseStatus
        }
}
