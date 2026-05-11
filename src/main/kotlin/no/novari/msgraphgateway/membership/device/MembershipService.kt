package no.novari.msgraphgateway.membership.device

import com.microsoft.graph.core.content.BatchRequestContent
import com.microsoft.graph.core.content.BatchResponseContent
import com.microsoft.graph.models.ReferenceCreate
import com.microsoft.graph.serviceclient.GraphServiceClient
import com.microsoft.kiota.RequestInformation
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.io.IOException
import java.time.OffsetDateTime
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

private const val UNKNOWN_ERROR = "Unknown error"

@Service
class MembershipService(
    private val graphServiceClient: GraphServiceClient,
    private val entraMembershipProducer: EntraMembershipProducer,
    private val deviceMembershipEntityRepository: DeviceMembershipEntityRepository,
    private val properties: DeviceMembershipProcessingProperties,
    private val defaultDispatcher: CoroutineDispatcher = Dispatchers.IO,
) {
    @Transactional
    fun deleteAllMemberships(): Int = deviceMembershipEntityRepository.deleteAll()

    @Transactional
    fun deleteMembershipsUpdatedBefore(cutoff: OffsetDateTime): Int =
        deviceMembershipEntityRepository.deleteLastUpdatedBefore(cutoff)

    fun processKontrollMembershipBatch(records: List<ConsumerRecord<String, DeviceResourceGroupMembership>>) {
        if (records.isEmpty()) {
            return
        }

        log.info("Received membership batch with {} records", records.size)
        val validMemberships = mutableListOf<ParsedMembership>()
        val pendingMemberships = mutableListOf<PendingMembership>()
        val statesToSave = mutableListOf<DeviceMembershipEntity>()
        val resultsToPublish = mutableListOf<MembershipResult>()

        records.forEach { record ->
            val messageKey = record.key()
            if (messageKey == null) {
                log.warn("Received membership with null key, skipping record")
                return@forEach
            }

            val membership = record.value()
            if (membership == null) {
                log.warn("Received null membership for key: {}", messageKey)
                return@forEach
            }

            validMemberships +=
                ParsedMembership(
                    messageKey,
                    membership,
                    parseMembershipId(membership, messageKey) ?: return@forEach,
                )
        }

        if (validMemberships.isEmpty()) {
            return
        }

        val existingMembershipsById =
            deviceMembershipEntityRepository.findAllByIds(
                validMemberships.map { it.membershipId }.distinct(),
            )

        validMemberships.forEach { parsed ->
            val existing = existingMembershipsById[parsed.membershipId]

            if (shouldSkipOperation(existing, parsed.membership.operation)) {
                val kafkaStatus = EntraStatus.NO_CHANGES
                val newMembershipStatus = getNewMembershipStatus(parsed.membership.operation, kafkaStatus)
                statesToSave += buildMembershipState(parsed.membershipId, existing, newMembershipStatus)
                resultsToPublish += MembershipResult(parsed.messageKey, parsed.membership, kafkaStatus)
                log.debug(
                    "Skipped duplicate membership operation {} for device {} and group {}",
                    parsed.membership.operation,
                    parsed.membership.entraDeviceRef,
                    parsed.membership.entraGroupRef,
                )
            } else {
                pendingMemberships +=
                    PendingMembership(
                        parsed.messageKey,
                        parsed.membership,
                        parsed.membershipId,
                        existing,
                    )
            }
        }

        val resolvedResults =
            runBlocking {
                processPendingMemberships(pendingMemberships)
            }

        resolvedResults.forEach { result ->
            val persistedStatus = getNewMembershipStatus(result.pending.membership.operation, result.status)
            statesToSave +=
                buildMembershipState(
                    result.pending.membershipId,
                    result.pending.existing,
                    persistedStatus,
                )
            resultsToPublish += MembershipResult(result.pending.messageKey, result.pending.membership, result.status)
        }

        deviceMembershipEntityRepository.saveAll(statesToSave)

        resultsToPublish.forEach { result ->
            publishResult(result.messageKey, result.membership, result.status)
        }
    }

    private fun processGraphBatchChunkWithRetries(chunk: List<PendingMembership>): List<ResolvedBatchResult> {
        var failed = chunk
        var retryCount = 0
        val resolved = mutableListOf<ResolvedBatchResult>()

        while (failed.isNotEmpty()) {
            val statusesByMembership = executeGraphBatch(failed)
            val toRetry = mutableListOf<PendingMembership>()

            failed.forEach { pending ->
                val status = statusesByMembership[pending] ?: EntraStatus.FAILED
                if (status == EntraStatus.FAILED && retryCount < MAX_RETRIES) {
                    toRetry += pending
                } else {
                    resolved += ResolvedBatchResult(pending, status)
                }
            }

            if (toRetry.isEmpty()) {
                return resolved
            }

            retryCount++
            log.warn(
                "Batch membership operations failed, retrying {} records ({}/{})",
                toRetry.size,
                retryCount,
                MAX_RETRIES,
            )
            failed = toRetry
        }

        return resolved
    }

    private suspend fun processPendingMemberships(
        pendingMemberships: List<PendingMembership>,
    ): List<ResolvedBatchResult> {
        if (pendingMemberships.isEmpty()) {
            return emptyList()
        }

        val chunks = pendingMemberships.chunked(properties.graphBatchSize)
        val resolvedByChunk = Array<List<ResolvedBatchResult>?>(chunks.size) { null }
        val nextChunkIndex = AtomicInteger(0)
        val workerCount = minOf(properties.graphMaxConcurrentCalls, chunks.size)

        coroutineScope {
            List(workerCount) {
                async(defaultDispatcher) {
                    while (true) {
                        val chunkIndex = nextChunkIndex.getAndIncrement()
                        if (chunkIndex >= chunks.size) {
                            return@async
                        }

                        resolvedByChunk[chunkIndex] = processGraphBatchChunkWithRetries(chunks[chunkIndex])
                    }
                }
            }.awaitAll()
        }

        return resolvedByChunk.filterNotNull().flatten()
    }

    private fun executeGraphBatch(memberships: List<PendingMembership>): Map<PendingMembership, EntraStatus> {
        val batchRequestContent = BatchRequestContent(graphServiceClient)
        val stepIdToMembership = linkedMapOf<String, PendingMembership>()

        memberships.forEach { membership ->
            val stepId = batchRequestContent.addBatchRequestStep(buildRequest(membership.membership))
            stepIdToMembership[stepId] = membership
        }

        val batchResponse =
            try {
                graphServiceClient.batchRequestBuilder.post(batchRequestContent, null)
            } catch (e: IOException) {
                log.error("I/O error while executing membership batch request", e)
                return memberships.associateWith { EntraStatus.FAILED }
            } catch (e: Exception) {
                log.error("Unexpected error while executing membership batch request", e)
                return memberships.associateWith { EntraStatus.FAILED }
            }

        val responseStatusCodes = batchResponse.responsesStatusCode.toMap()
        return stepIdToMembership.entries.associate { (stepId, membership) ->
            val statusCode = responseStatusCodes[stepId]
            if (statusCode == null) {
                log.error(
                    "Missing batch response status for stepId {} (device {}, group {})",
                    stepId,
                    membership.membership.entraDeviceRef,
                    membership.membership.entraGroupRef,
                )
                membership to EntraStatus.FAILED
            } else {
                val error = readMessage(batchResponse, stepId, statusCode)
                membership to toEntraStatus(membership.membership, statusCode, error)
            }
        }
    }

    private fun buildRequest(membership: DeviceResourceGroupMembership): RequestInformation =
        when (membership.operation) {
            OperationType.ADD -> buildAddRequest(membership)
            OperationType.REMOVE -> buildRemoveRequest(membership)
        }

    private fun buildAddRequest(membership: DeviceResourceGroupMembership): RequestInformation {
        val referenceMember = ReferenceCreate()
        referenceMember.odataId = properties.directoryObjectsBaseUrl + membership.entraDeviceRef
        return graphServiceClient
            .groups()
            .byGroupId(membership.entraGroupRef)
            .members()
            .ref()
            .toPostRequestInformation(referenceMember)
    }

    private fun buildRemoveRequest(membership: DeviceResourceGroupMembership): RequestInformation =
        graphServiceClient
            .groups()
            .byGroupId(membership.entraGroupRef)
            .members()
            .byDirectoryObjectId(membership.entraDeviceRef)
            .ref()
            .toDeleteRequestInformation()

    private fun readMessage(
        batchResponse: BatchResponseContent,
        stepId: String,
        statusCode: Int,
    ): String? {
        if (BatchResponseContent.isSuccessStatusCode(statusCode)) {
            return null
        }

        return batchResponse.getResponseById(stepId)?.message
    }

    private fun toEntraStatus(
        membership: DeviceResourceGroupMembership,
        statusCode: Int,
        error: String?,
    ): EntraStatus =
        when (membership.operation) {
            OperationType.ADD -> toAddStatus(membership, statusCode, error)
            OperationType.REMOVE -> toRemoveStatus(membership, statusCode, error)
        }

    private fun toAddStatus(
        membership: DeviceResourceGroupMembership,
        statusCode: Int,
        error: String?,
    ): EntraStatus {
        if (BatchResponseContent.isSuccessStatusCode(statusCode)) {
            return EntraStatus.ADDED
        }
        if (statusCode == 400 && error?.contains("object references already exist", ignoreCase = true) == true) {
            log.warn(
                "Device with ID {} already a member of group with ID {}",
                membership.entraDeviceRef,
                membership.entraGroupRef,
            )
            return EntraStatus.NO_CHANGES
        }

        if (statusCode == 400 && error != null) {
            log.warn(
                "Error adding device with ID {} to group with ID {}: {}",
                membership.entraDeviceRef,
                membership.entraGroupRef,
                error,
            )
            return EntraStatus.ERROR
        }

        if (statusCode == 404) {
            log.warn(
                "DeviceId: {} cannot be added to GroupId: {}. Error: {}",
                membership.entraDeviceRef,
                membership.entraGroupRef,
                error ?: UNKNOWN_ERROR,
            )
            return EntraStatus.ERROR
        }

        if (statusCode == 429) {
            log.warn(
                "Throttling limit while adding device {} to group {}",
                membership.entraDeviceRef,
                membership.entraGroupRef,
            )
            return EntraStatus.FAILED
        }

        log.warn(
            "HTTP error while updating group {} in batch: status={} message={}",
            membership.entraGroupRef,
            statusCode,
            error ?: UNKNOWN_ERROR,
        )
        return EntraStatus.FAILED
    }

    private fun toRemoveStatus(
        membership: DeviceResourceGroupMembership,
        statusCode: Int,
        error: String?,
    ): EntraStatus {
        if (BatchResponseContent.isSuccessStatusCode(statusCode)) {
            return EntraStatus.REMOVED
        }

        if (statusCode == 404) {
            log.warn(
                "Delete received for DeviceId: {} in GroupId: {}. Device not found in group, publishing 'removed' event to Kafka to keep state consistent.",
                membership.entraDeviceRef,
                membership.entraGroupRef,
            )
            return EntraStatus.REMOVED
        }

        if (statusCode == 429) {
            log.warn(
                "Throttling limit while removing device {} from group {}",
                membership.entraDeviceRef,
                membership.entraGroupRef,
            )
            return EntraStatus.FAILED
        }

        log.error(
            "HTTP error while trying to remove device {} from group {} in batch. status={} message={}",
            membership.entraDeviceRef,
            membership.entraGroupRef,
            statusCode,
            error ?: UNKNOWN_ERROR,
        )
        return EntraStatus.FAILED
    }

    private fun parseMembershipId(
        membership: DeviceResourceGroupMembership,
        messageKey: String,
    ): DeviceMembershipId? =
        try {
            DeviceMembershipId(
                UUID.fromString(membership.entraDeviceRef),
                UUID.fromString(membership.entraGroupRef),
            )
        } catch (_: IllegalArgumentException) {
            log.error(
                "Invalid device/group UUIDs in membership message key {} (deviceRef={}, groupRef={})",
                messageKey,
                membership.entraDeviceRef,
                membership.entraGroupRef,
            )
            publishResult(messageKey, membership, EntraStatus.ERROR)
            return null
        }

    private fun shouldSkipOperation(
        existing: DeviceMembershipEntity?,
        operation: OperationType,
    ): Boolean {
        if (existing == null) {
            return false
        }

        return when (operation) {
            OperationType.ADD -> existing.status == EntraStatus.ADDED
            OperationType.REMOVE -> existing.status == EntraStatus.REMOVED
        }
    }

    private fun getNewMembershipStatus(
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

    private fun buildMembershipState(
        id: DeviceMembershipId,
        existing: DeviceMembershipEntity?,
        status: EntraStatus,
    ): DeviceMembershipEntity {
        val now = OffsetDateTime.now()
        return DeviceMembershipEntity(
            id = id,
            status = status,
            createdAt = existing?.createdAt ?: now,
            lastUpdatedAt = now,
        )
    }

    private fun publishResult(
        messageKey: String,
        membership: DeviceResourceGroupMembership,
        status: EntraStatus,
    ) {
        val entraDeviceMembership =
            EntraDeviceMembership(
                status,
                membership.entraGroupRef,
                membership.entraDeviceRef,
            )
        entraMembershipProducer.publish(messageKey, entraDeviceMembership)
    }

    companion object {
        private val log = LoggerFactory.getLogger(MembershipService::class.java)
        private const val MAX_RETRIES = 2
    }

    private data class PendingMembership(
        val messageKey: String,
        val membership: DeviceResourceGroupMembership,
        val membershipId: DeviceMembershipId,
        val existing: DeviceMembershipEntity?,
    )

    private data class ParsedMembership(
        val messageKey: String,
        val membership: DeviceResourceGroupMembership,
        val membershipId: DeviceMembershipId,
    )

    private data class MembershipResult(
        val messageKey: String,
        val membership: DeviceResourceGroupMembership,
        val status: EntraStatus,
    )

    private data class ResolvedBatchResult(
        val pending: PendingMembership,
        val status: EntraStatus,
    )
}
