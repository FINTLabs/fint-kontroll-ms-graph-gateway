package no.novari.msgraphgateway.services.group

import com.microsoft.graph.models.Group
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import no.novari.msgraphgateway.config.ConfigGroup
import no.novari.msgraphgateway.entra.group.EntraGroup
import no.novari.msgraphgateway.kafka.group.GroupProducerService
import no.novari.msgraphgateway.repository.group.GroupRepository
import no.novari.msgraphgateway.repository.group.GroupStateRepository
import no.novari.msgraphgateway.services.Checksum
import no.novari.msgraphgateway.services.ChecksumService
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Instant
import java.util.UUID

@Service
class EntraGroupSyncService(
    private val groupRepository: GroupRepository,
    private val checksumService: ChecksumService,
    private val producer: GroupProducerService,
    private val configGroup: ConfigGroup,
) {
    private val batchSize = 1000
    private val checksumPermits = Semaphore(32)
    private val dbBatchPermits = Semaphore(2)
    private val kafkaPermits = Semaphore(100)

    suspend fun processPage(
        groups: List<Group>?,
        notSeenIncremented: MutableSet<UUID>,
        republishAll: Boolean,
    ): Int {
        if (groups.isNullOrEmpty()) return 0

        var publishedTotal = 0

        for (batch in groups.chunked(batchSize)) {
            publishedTotal += processBatch(batch, notSeenIncremented, republishAll)
        }

        return publishedTotal
    }

    suspend fun finishFullImport(cutoff: Instant): Int {
        val deletableIds =
            withContext(Dispatchers.IO) {
                groupRepository.findStaleObjectIdsWithNotSeenCountGreaterThan(
                    cutoff,
                    configGroup.minNotSeenCount,
                )
            }

        log.info("Found {} stale groups eligible for deletion", deletableIds.size)

        if (deletableIds.isEmpty()) return 0

        var deletedTotal = 0

        for (batch in deletableIds.chunked(batchSize)) {
            val deletedRows =
                withContext(Dispatchers.IO) {
                    groupRepository.deleteByIdsReturningRows(batch)
                }

            deletedTotal += deletedRows.size

            coroutineScope {
                deletedRows
                    .map { row ->
                        async(Dispatchers.IO) {
                            kafkaPermits.withPermit {
                                producer.publishDeletedGroup(
                                    groupId = row.objectId.toString(),
                                    resourceGroupId = row.resourceGroupId,
                                )
                            }
                        }
                    }.awaitAll()
            }
        }

        return deletedTotal
    }

    suspend fun markNotSeenGroups(
        cutoff: Instant,
        notSeenIncremented: MutableSet<UUID>,
    ): Int {
        val staleGroupIds =
            withContext(Dispatchers.IO) {
                groupRepository.findStaleObjectIds(cutoff)
            }.filter { notSeenIncremented.add(it) }

        log.info("Marking {} groups missing from full import as not seen", staleGroupIds.size)

        withContext(Dispatchers.IO) {
            groupRepository.incrementNotSeenCount(staleGroupIds)
        }

        return staleGroupIds.size
    }

    private suspend fun processBatch(
        batch: List<Group>,
        notSeenIncremented: MutableSet<UUID>,
        republishAll: Boolean,
    ): Int =
        coroutineScope {
            val now = Instant.now()

            val removedGroups = batch.filter { it.additionalData.containsKey("@removed") }

            if (removedGroups.isNotEmpty()) {
                log.info("There are {} removed groups", removedGroups.size)

                var markedRemovedGroups = 0
                removedGroups.forEach { group ->
                    if (handleRemoved(group.id, notSeenIncremented)) {
                        markedRemovedGroups++
                    }
                }

                log.info("Marked {} removed groups as not seen", markedRemovedGroups)
            }

            val candidates =
                batch
                    .asSequence()
                    .filterNot { it.additionalData.containsKey("@removed") }
                    .filter(::matchesConfiguredGroup)
                    .mapNotNull { group ->
                        val id = parseObjectIdOrNull(group.id) ?: return@mapNotNull null
                        id to group
                    }.distinctBy { it.first }
                    .toList()

            if (candidates.isEmpty()) return@coroutineScope 0

            if (republishAll) {
                upsertAndPublishAll(
                    now = now,
                    candidates = candidates,
                    toDto = { group -> EntraGroup(group, configGroup) },
                    checksum = { dto -> checksumService.checksum(dto) },
                    publish = { dto -> producer.publish(dto) },
                    logLabel = "groups",
                )
            } else {
                upsertAndPublishChanged(
                    now = now,
                    candidates = candidates,
                    toDto = { group -> EntraGroup(group, configGroup) },
                    checksum = { dto -> checksumService.checksum(dto) },
                    publish = { dto -> producer.publish(dto) },
                    logLabel = "groups",
                )
            }
        }

    private suspend fun <DTO : Any> upsertAndPublishChanged(
        now: Instant,
        candidates: List<Pair<UUID, Group>>,
        toDto: (Group) -> DTO,
        checksum: (DTO) -> Checksum,
        publish: suspend (DTO) -> Unit,
        logLabel: String,
    ): Int =
        coroutineScope {
            if (candidates.isEmpty()) return@coroutineScope 0

            val prepared =
                prepareRowsAndDtos(
                    now = now,
                    candidates = candidates,
                    toDto = toDto,
                    checksum = checksum,
                )

            if (prepared.rows.isEmpty()) return@coroutineScope 0

            val changedIds: Set<UUID> =
                dbBatchPermits.withPermit {
                    withContext(Dispatchers.IO) {
                        groupRepository.batchUpsertReturningChanged(prepared.rows)
                    }
                }

            if (changedIds.isNotEmpty()) {
                log.debug("There are {} changed {}", changedIds.size, logLabel)
            }

            val jobs =
                changedIds.mapNotNull { id ->
                    val dto = prepared.dtoById[id] ?: return@mapNotNull null

                    async(Dispatchers.IO) {
                        runCatching {
                            kafkaPermits.withPermit {
                                publish(dto)
                            }
                        }.onFailure {
                            log.warn("Failed publishing {} {}", logLabel, id, it)
                        }
                    }
                }

            jobs.awaitAll().count { it.isSuccess }
        }

    private suspend fun <DTO : Any> upsertAndPublishAll(
        now: Instant,
        candidates: List<Pair<UUID, Group>>,
        toDto: (Group) -> DTO,
        checksum: (DTO) -> Checksum,
        publish: suspend (DTO) -> Unit,
        logLabel: String,
    ): Int =
        coroutineScope {
            if (candidates.isEmpty()) return@coroutineScope 0

            val prepared =
                prepareRowsAndDtos(
                    now = now,
                    candidates = candidates,
                    toDto = toDto,
                    checksum = checksum,
                )

            if (prepared.rows.isEmpty()) return@coroutineScope 0

            dbBatchPermits.withPermit {
                withContext(Dispatchers.IO) {
                    groupRepository.batchUpsert(prepared.rows)
                }
            }

            val jobs =
                prepared.rows.mapNotNull { row ->
                    val dto = prepared.dtoById[row.objectId] ?: return@mapNotNull null

                    async(Dispatchers.IO) {
                        runCatching {
                            kafkaPermits.withPermit {
                                publish(dto)
                            }
                        }.onFailure {
                            log.warn("Failed publishing {} {}", logLabel, row.objectId, it)
                        }
                    }
                }

            jobs.awaitAll().count { it.isSuccess }
        }

    private suspend fun handleRemoved(
        groupId: String?,
        notSeenIncremented: MutableSet<UUID>,
    ): Boolean {
        if (groupId.isNullOrBlank()) return false

        val objectId = parseObjectIdOrNull(groupId) ?: return false

        if (!notSeenIncremented.add(objectId)) {
            log.debug("Removed group {} already marked not seen in this run; skipping", objectId)
            return false
        }

        val exists =
            runCatching {
                withContext(Dispatchers.IO) {
                    groupRepository.existsById(objectId)
                }
            }.getOrDefault(false)

        if (exists) {
            withContext(Dispatchers.IO) {
                groupRepository.incrementNotSeenCount(listOf(objectId))
            }

            log.debug("Marked group {} as not seen (+1) due to @removed", objectId)
            return true
        } else {
            log.debug("Removed group {} not found in DB; skipping", objectId)
            return false
        }
    }

    private suspend fun <DTO : Any> prepareRowsAndDtos(
        now: Instant,
        candidates: List<Pair<UUID, Group>>,
        toDto: (Group) -> DTO,
        checksum: (DTO) -> Checksum,
    ): PreparedBatch<DTO> =
        coroutineScope {
            data class Computed<DTO>(
                val id: UUID,
                val resourceGroupId: Long,
                val dto: DTO,
                val checksum: Checksum,
            )

            val validCandidates =
                candidates
                    .mapNotNull { (id, group) ->
                        val resourceGroupId =
                            group.resourceGroupIdOrNull()
                                ?: run {
                                    log.debug("Skipping group {} because resourceGroupId is missing or invalid", id)
                                    return@mapNotNull null
                                }

                        GroupCandidate(
                            id = id,
                            group = group,
                            resourceGroupId = resourceGroupId,
                        )
                    }.selectUsableCandidates()

            val computed =
                validCandidates
                    .map { candidate ->
                        async(Dispatchers.Default) {
                            val dto = toDto(candidate.group)

                            checksumPermits.withPermit {
                                Computed(
                                    id = candidate.id,
                                    resourceGroupId = candidate.resourceGroupId,
                                    dto = dto,
                                    checksum = checksum(dto),
                                )
                            }
                        }
                    }.awaitAll()

            val rows = ArrayList<GroupStateRepository.UpsertRow>(computed.size)
            val dtoById = HashMap<UUID, DTO>(computed.size)

            for (c in computed) {
                rows +=
                    GroupStateRepository.UpsertRow(
                        objectId = c.id,
                        resourceGroupId = c.resourceGroupId,
                        checksum = c.checksum,
                        lastSeenAt = now,
                    )
                dtoById[c.id] = c.dto
            }

            PreparedBatch(rows = rows, dtoById = dtoById)
        }

    private suspend fun List<GroupCandidate>.selectUsableCandidates(): List<GroupCandidate> {
        if (isEmpty()) return emptyList()

        val candidatesByResourceGroupId = groupBy { it.resourceGroupId }

        if (candidatesByResourceGroupId.values.all { it.size == 1 }) {
            return this
        }

        val selected = ArrayList<GroupCandidate>(size)

        candidatesByResourceGroupId.forEach { (resourceGroupId, candidates) ->
            if (candidates.size == 1) {
                selected += candidates.first()
                return@forEach
            }

            val storedObjectId =
                withContext(Dispatchers.IO) {
                    groupRepository.findObjectIdByResourceGroupId(resourceGroupId)
                }
            val storedCandidate = storedObjectId?.let { objectId -> candidates.firstOrNull { it.id == objectId } }
            val duplicateIds = candidates.joinToString { it.id.toString() }

            if (storedCandidate != null) {
                log.error(
                    "Duplicate resourceGroupId {} found for Entra groups [{}]. " +
                        "Updating existing group {} only. Clean up duplicate resourceGroupId in Entra before these groups can be used.",
                    resourceGroupId,
                    duplicateIds,
                    storedCandidate.id,
                )
                selected += storedCandidate
            } else {
                log.error(
                    "Duplicate resourceGroupId {} found for Entra groups [{}]. " +
                        "No matching existing group is stored; skipping all of them. " +
                        "Clean up duplicate resourceGroupId in Entra before these groups can be used.",
                    resourceGroupId,
                    duplicateIds,
                )
            }
        }

        return selected
    }

    private data class GroupCandidate(
        val id: UUID,
        val group: Group,
        val resourceGroupId: Long,
    )

    private data class PreparedBatch<DTO : Any>(
        val rows: List<GroupStateRepository.UpsertRow>,
        val dtoById: Map<UUID, DTO>,
    )

    private fun Group.resourceGroupIdOrNull(): Long? {
        val attr =
            configGroup.resourceGroupIdAttribute
                ?.takeIf { it.isNotBlank() }
                ?: return null

        return additionalData[attr]
            ?.toString()
            ?.toLongOrNull()
    }

    fun matchesConfiguredGroup(group: Group): Boolean {
        val name = group.displayName ?: return false

        val prefix = configGroup.prefix?.trim().orEmpty()
        val suffix = configGroup.suffix?.trim().orEmpty()
        val mode = configGroup.filterMode

        val matchesName =
            when (mode) {
                ConfigGroup.FilterMode.NONE -> {
                    true
                }

                ConfigGroup.FilterMode.PREFIX -> {
                    prefix.isNotBlank() && name.startsWith(prefix, ignoreCase = true)
                }

                ConfigGroup.FilterMode.SUFFIX -> {
                    suffix.isNotBlank() && name.endsWith(suffix, ignoreCase = true)
                }

                ConfigGroup.FilterMode.BOTH -> {
                    prefix.isNotBlank() &&
                        suffix.isNotBlank() &&
                        name.startsWith(prefix, ignoreCase = true) &&
                        name.endsWith(suffix, ignoreCase = true)
                }
            }

        val hasResourceGroupId =
            configGroup.resourceGroupIdAttribute
                ?.takeIf { it.isNotBlank() }
                ?.let { attr -> group.additionalData.containsKey(attr) }
                ?: true

        return matchesName && hasResourceGroupId
    }

    private fun parseObjectIdOrNull(groupId: String?): UUID? =
        if (groupId.isNullOrBlank()) null else runCatching { UUID.fromString(groupId) }.getOrNull()

    companion object {
        private val log = LoggerFactory.getLogger(EntraGroupSyncService::class.java)
    }
}
