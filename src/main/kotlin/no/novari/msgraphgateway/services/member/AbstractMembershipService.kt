package no.novari.msgraphgateway.services.member

import com.microsoft.graph.core.content.BatchRequestContent
import com.microsoft.graph.core.content.BatchResponseContent
import com.microsoft.graph.models.ReferenceCreate
import com.microsoft.graph.serviceclient.GraphServiceClient
import com.microsoft.kiota.RequestInformation
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import no.novari.msgraphgateway.entra.EntraStatus
import no.novari.msgraphgateway.kafka.OperationType
import no.novari.msgraphgateway.membership.MembershipProcessingProperties
import no.novari.msgraphgateway.membership.MembershipStatusResolver
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

private const val UNKNOWN_ERROR = "Unknown error"
private const val MAX_RETRIES = 2

abstract class AbstractMembershipService<M : Any, ID : Any, E : Any>(
    private val graphServiceClient: GraphServiceClient,
    private val properties: MembershipProcessingProperties,
    private val defaultDispatcher: CoroutineDispatcher,
) {
    open fun processKontrollMembershipBatch(records: List<ConsumerRecord<String, M>>) {
        if (records.isEmpty()) {
            return
        }

        log.info("Received membership batch with {} records", records.size)
        val validMemberships = mutableListOf<ParsedMembership<M, ID>>()
        val pendingMemberships = mutableListOf<PendingMembership<M, ID, E>>()
        val statesToSave = mutableListOf<E>()
        val resultsToPublish = mutableListOf<MembershipResult<M>>()

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
            findExistingMemberships(
                validMemberships.map { it.membershipId }.distinct(),
            )

        validMemberships.forEach { parsed ->
            val existing = existingMembershipsById[parsed.membershipId]

            if (
                MembershipStatusResolver.shouldSkipOperation(
                    existing?.let(::statusOf),
                    operationOf(parsed.membership),
                )
            ) {
                val kafkaStatus = EntraStatus.NO_CHANGES
                val persistedStatus =
                    MembershipStatusResolver.persistedStatus(
                        operationOf(parsed.membership),
                        kafkaStatus,
                    )
                statesToSave += buildMembershipState(parsed.membershipId, existing, persistedStatus)
                resultsToPublish += MembershipResult(parsed.messageKey, parsed.membership, kafkaStatus)
                log.debug(
                    "Skipped duplicate membership operation {} for {} {} and group {}",
                    operationOf(parsed.membership),
                    memberType,
                    memberIdOf(parsed.membership),
                    groupIdOf(parsed.membership),
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
            val persistedStatus =
                MembershipStatusResolver.persistedStatus(
                    operationOf(result.pending.membership),
                    result.status,
                )
            statesToSave +=
                buildMembershipState(
                    result.pending.membershipId,
                    result.pending.existing,
                    persistedStatus,
                )
            resultsToPublish += MembershipResult(result.pending.messageKey, result.pending.membership, result.status)
        }

        saveMembershipStates(statesToSave)

        resultsToPublish.forEach { result ->
            publishResult(result.messageKey, result.membership, result.status)
        }
    }

    private fun parseMembershipId(
        membership: M,
        messageKey: String,
    ): ID? =
        try {
            membershipIdOf(
                UUID.fromString(memberIdOf(membership)),
                UUID.fromString(groupIdOf(membership)),
            )
        } catch (_: IllegalArgumentException) {
            log.error(
                "Invalid {}/group UUIDs in membership message key {} ({}Ref={}, groupRef={})",
                memberType,
                messageKey,
                memberType,
                memberIdOf(membership),
                groupIdOf(membership),
            )
            publishResult(messageKey, membership, EntraStatus.ERROR)
            null
        }

    private fun processGraphBatchChunkWithRetries(
        chunk: List<PendingMembership<M, ID, E>>,
    ): List<ResolvedBatchResult<M, ID, E>> {
        var failed = chunk
        var retryCount = 0
        val resolved = mutableListOf<ResolvedBatchResult<M, ID, E>>()

        while (failed.isNotEmpty()) {
            val statusesByMembership = executeGraphBatch(failed)
            val toRetry = mutableListOf<PendingMembership<M, ID, E>>()

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
        pendingMemberships: List<PendingMembership<M, ID, E>>,
    ): List<ResolvedBatchResult<M, ID, E>> {
        if (pendingMemberships.isEmpty()) {
            return emptyList()
        }

        val chunks = pendingMemberships.chunked(properties.graphBatchSize)
        val resolvedByChunk = Array<List<ResolvedBatchResult<M, ID, E>>?>(chunks.size) { null }
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

    private fun executeGraphBatch(
        memberships: List<PendingMembership<M, ID, E>>,
    ): Map<PendingMembership<M, ID, E>, EntraStatus> {
        val batchRequestContent = BatchRequestContent(graphServiceClient)
        val stepIdToMembership = linkedMapOf<String, PendingMembership<M, ID, E>>()

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
                    "Missing batch response status for stepId {} ({} {}, group {})",
                    stepId,
                    memberType,
                    memberIdOf(membership.membership),
                    groupIdOf(membership.membership),
                )
                membership to EntraStatus.FAILED
            } else {
                val error = readMessage(batchResponse, stepId, statusCode)
                membership to toEntraStatus(membership.membership, statusCode, error)
            }
        }
    }

    private fun buildRequest(membership: M): RequestInformation =
        when (operationOf(membership)) {
            OperationType.ADD -> buildAddRequest(membership)
            OperationType.REMOVE -> buildRemoveRequest(membership)
        }

    private fun buildAddRequest(membership: M): RequestInformation {
        val referenceMember = ReferenceCreate()
        referenceMember.odataId = properties.directoryObjectsBaseUrl + memberIdOf(membership)
        return graphServiceClient
            .groups()
            .byGroupId(groupIdOf(membership))
            .members()
            .ref()
            .toPostRequestInformation(referenceMember)
    }

    private fun buildRemoveRequest(membership: M): RequestInformation =
        graphServiceClient
            .groups()
            .byGroupId(groupIdOf(membership))
            .members()
            .byDirectoryObjectId(memberIdOf(membership))
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
        membership: M,
        statusCode: Int,
        error: String?,
    ): EntraStatus =
        when (operationOf(membership)) {
            OperationType.ADD -> toAddStatus(membership, statusCode, error)
            OperationType.REMOVE -> toRemoveStatus(membership, statusCode, error)
        }

    private fun toAddStatus(
        membership: M,
        statusCode: Int,
        error: String?,
    ): EntraStatus {
        if (BatchResponseContent.isSuccessStatusCode(statusCode)) {
            return EntraStatus.ADDED
        }
        if (statusCode == 400 && error?.contains("object references already exist", ignoreCase = true) == true) {
            log.warn(
                "{} with ID {} already a member of group with ID {}",
                memberTypeForLog,
                memberIdOf(membership),
                groupIdOf(membership),
            )
            return EntraStatus.NO_CHANGES
        }

        if (statusCode == 400 && error != null) {
            log.warn(
                "Error adding {} with ID {} to group with ID {}: {}",
                memberType,
                memberIdOf(membership),
                groupIdOf(membership),
                error,
            )
            return EntraStatus.ERROR
        }

        if (statusCode == 404) {
            log.warn(
                "{}Id: {} cannot be added to GroupId: {}. Error: {}",
                memberTypeForLog,
                memberIdOf(membership),
                groupIdOf(membership),
                error ?: UNKNOWN_ERROR,
            )
            return EntraStatus.ERROR
        }

        if (statusCode == 429) {
            log.warn(
                "Throttling limit while adding {} {} to group {}",
                memberType,
                memberIdOf(membership),
                groupIdOf(membership),
            )
            return EntraStatus.FAILED
        }

        log.warn(
            "HTTP error while updating group {} in batch: status={} message={}",
            groupIdOf(membership),
            statusCode,
            error ?: UNKNOWN_ERROR,
        )
        return EntraStatus.FAILED
    }

    private fun toRemoveStatus(
        membership: M,
        statusCode: Int,
        error: String?,
    ): EntraStatus {
        if (BatchResponseContent.isSuccessStatusCode(statusCode)) {
            return EntraStatus.REMOVED
        }

        if (statusCode == 404) {
            log.warn(
                "Delete received for {}Id: {} in GroupId: {}. {} not found in group, publishing 'removed' event to Kafka to keep state consistent.",
                memberTypeForLog,
                memberIdOf(membership),
                groupIdOf(membership),
                memberTypeForLog,
            )
            return EntraStatus.REMOVED
        }

        if (statusCode == 429) {
            log.warn(
                "Throttling limit while removing {} {} from group {}",
                memberType,
                memberIdOf(membership),
                groupIdOf(membership),
            )
            return EntraStatus.FAILED
        }

        log.error(
            "HTTP error while trying to remove {} {} from group {} in batch. status={} message={}",
            memberType,
            memberIdOf(membership),
            groupIdOf(membership),
            statusCode,
            error ?: UNKNOWN_ERROR,
        )
        return EntraStatus.FAILED
    }

    protected abstract val memberType: String
    protected abstract val memberTypeForLog: String
    protected abstract fun operationOf(membership: M): OperationType
    protected abstract fun groupIdOf(membership: M): String
    protected abstract fun memberIdOf(membership: M): String
    protected abstract fun membershipIdOf(memberId: UUID, groupId: UUID): ID
    protected abstract fun findExistingMemberships(ids: Collection<ID>): Map<ID, E>
    protected abstract fun statusOf(existing: E): EntraStatus
    protected abstract fun buildMembershipState(id: ID, existing: E?, status: EntraStatus): E
    protected abstract fun saveMembershipStates(states: Collection<E>)
    protected abstract fun publishResult(messageKey: String, membership: M, status: EntraStatus)

    private data class PendingMembership<M : Any, ID : Any, E : Any>(
        val messageKey: String,
        val membership: M,
        val membershipId: ID,
        val existing: E?,
    )

    private data class ParsedMembership<M : Any, ID : Any>(
        val messageKey: String,
        val membership: M,
        val membershipId: ID,
    )

    private data class MembershipResult<M : Any>(
        val messageKey: String,
        val membership: M,
        val status: EntraStatus,
    )

    private data class ResolvedBatchResult<M : Any, ID : Any, E : Any>(
        val pending: PendingMembership<M, ID, E>,
        val status: EntraStatus,
    )

    companion object {
        private val log = LoggerFactory.getLogger(AbstractMembershipService::class.java)
    }
}
