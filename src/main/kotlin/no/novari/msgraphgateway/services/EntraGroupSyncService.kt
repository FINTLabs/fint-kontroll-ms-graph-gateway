package no.novari.msgraphgateway.services

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

        log.info("Found {} stale groups", deletableIds.size)

        if (deletableIds.isEmpty()) return 0

        var deletedTotal = 0

        for (batch in deletableIds.chunked(batchSize)) {
            val deletedObjectIds =
                withContext(Dispatchers.IO) {
                    groupRepository.deleteByIdsReturningObjectIds(batch)
                }

            deletedTotal += deletedObjectIds.size

            coroutineScope {
                deletedObjectIds
                    .map { objectId ->
                        async(Dispatchers.IO) {
                            kafkaPermits.withPermit {
                                producer.publishDeletedGroup(objectId.toString())
                            }
                        }
                    }.awaitAll()
            }
        }

        return deletedTotal
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

                removedGroups.forEach { group ->
                    handleRemoved(group.id, notSeenIncremented)
                }
            }

            val candidates =
                batch
                    .asSequence()
                    .filterNot { it.additionalData.containsKey("@removed") }
                    .filter { it.matchesConfiguredGroup() }
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
    ) {
        if (groupId.isNullOrBlank()) return

        val objectId = parseObjectIdOrNull(groupId) ?: return

        if (!notSeenIncremented.add(objectId)) {
            log.debug("Removed group {} already marked not seen in this run; skipping", objectId)
            return
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
        } else {
            log.debug("Removed group {} not found in DB; skipping", objectId)
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
                val dto: DTO,
                val checksum: Checksum,
            )

            val computed =
                candidates
                    .map { (id, group) ->
                        async(Dispatchers.Default) {
                            val dto = toDto(group)

                            checksumPermits.withPermit {
                                Computed(id, dto, checksum(dto))
                            }
                        }
                    }.awaitAll()

            val rows = ArrayList<GroupStateRepository.UpsertRow>(computed.size)
            val dtoById = HashMap<UUID, DTO>(computed.size)

            for (c in computed) {
                rows += GroupStateRepository.UpsertRow(c.id, c.checksum, now)
                dtoById[c.id] = c.dto
            }

            PreparedBatch(rows = rows, dtoById = dtoById)
        }

    private data class PreparedBatch<DTO : Any>(
        val rows: List<GroupStateRepository.UpsertRow>,
        val dtoById: Map<UUID, DTO>,
    )

    private fun Group.matchesConfiguredGroup(): Boolean {
        val name = displayName ?: return false

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
                ?.let { attr -> additionalData.containsKey(attr) }
                ?: true

        return matchesName && hasResourceGroupId
    }

    private fun parseObjectIdOrNull(groupId: String?): UUID? =
        if (groupId.isNullOrBlank()) null else runCatching { UUID.fromString(groupId) }.getOrNull()

    companion object {
        private val log = LoggerFactory.getLogger(EntraGroupSyncService::class.java)
    }
}
