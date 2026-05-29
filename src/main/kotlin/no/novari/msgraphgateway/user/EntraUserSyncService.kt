package no.novari.msgraphgateway.user

import com.microsoft.graph.models.User
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import no.novari.msgraphgateway.config.ConfigUser
import no.novari.msgraphgateway.entra.EntraUser
import no.novari.msgraphgateway.entra.EntraUserExternal
import no.novari.msgraphgateway.kafka.UserExternalProducerService
import no.novari.msgraphgateway.kafka.UserProducerService
import no.novari.msgraphgateway.service.Checksum
import no.novari.msgraphgateway.service.ChecksumService
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Instant
import java.util.*

@Service
class EntraUserSyncService(
    private val userRepository: UserRepository,
    private val userExternalRepository: UserExternalRepository,
    private val checksumService: ChecksumService,
    private val producer: UserProducerService,
    private val externalProducer: UserExternalProducerService,
    private val configUser: ConfigUser,
) {
    private val batchSize = 1000
    private val checksumPermits = Semaphore(32)
    private val dbBatchPermits = Semaphore(2)
    private val kafkaPermits = Semaphore(100)

    suspend fun processPage(
        users: List<User>?,
        notSeenIncremented: MutableSet<UUID>,
        republishAll: Boolean,
        correlationId: String? = null,
    ): Int {
        if (users.isNullOrEmpty()) return 0
        var publishedTotal = 0
        for (batch in users.chunked(batchSize)) {
            publishedTotal += processBatch(batch, notSeenIncremented, republishAll, correlationId)
        }
        return publishedTotal
    }

    suspend fun finishFullImport(cutoff: Instant): Int =
        finishFullImportFor(
            repo = userRepository,
            cutoff = cutoff,
            publishDeleted = { id -> producer.publishDeletedUser(id) },
            label = "users",
        )

    suspend fun finishFullImportExternal(cutoff: Instant): Int =
        finishFullImportFor(
            repo = userExternalRepository,
            cutoff = cutoff,
            publishDeleted = { id -> externalProducer.publishDeletedUser(id) },
            label = "external users",
        )

    private suspend fun finishFullImportFor(
        repo: UserStateRepository,
        cutoff: Instant,
        publishDeleted: suspend (String) -> Unit,
        label: String,
    ): Int {
        val deletableIds =
            withContext(Dispatchers.IO) {
                repo.findStaleObjectIdsWithNotSeenCountGreaterThan(cutoff, configUser.minNotSeenCount)
            }
        log.info("Found {} stale {}", deletableIds.size, label)
        if (deletableIds.isEmpty()) return 0

        var deletedTotal = 0
        for (batch in deletableIds.chunked(batchSize)) {
            val deletedObjectIds =
                withContext(Dispatchers.IO) {
                    repo.deleteByIdsReturningObjectIds(batch)
                }
            deletedTotal += deletedObjectIds.size

            coroutineScope {
                deletedObjectIds
                    .map { objectId ->
                        async(Dispatchers.IO) {
                            kafkaPermits.withPermit {
                                runCatching {
                                    log.trace("Publishing deleted {} event (entraId={})", label, objectId)
                                    publishDeleted(objectId.toString())
                                }.onSuccess {
                                    log.trace("Published deleted {} event (entraId={})", label, objectId)
                                }.onFailure {
                                    log.warn("Failed publishing deleted {} event (entraId={})", label, objectId, it)
                                }.getOrThrow()
                            }
                        }
                    }.awaitAll()
            }
        }
        return deletedTotal
    }

    private suspend fun processBatch(
        batch: List<User>,
        notSeenIncremented: MutableSet<UUID>,
        republishAll: Boolean,
        correlationId: String?,
    ): Int =
        coroutineScope {
            val now = Instant.now()
            log.trace(
                "Processing user batch (correlationId={}, size={}, republishAll={}, entraIds={})",
                correlationId,
                batch.size,
                republishAll,
                batch.mapNotNull { it.id }.joinToString(","),
            )

            val removedUsers = batch.filter { it.additionalData.containsKey("@removed") }
            if (removedUsers.isNotEmpty()) {
                log.info("There are {} removed users", removedUsers.size)
                removedUsers.forEach { u ->
                    log.trace(
                        "Received removed user from Microsoft Graph (correlationId={}, entraId={})",
                        correlationId,
                        u.id,
                    )
                    handleRemoved(u.id, notSeenIncremented)
                }
            }

            val candidates: List<Pair<UUID, User>> =
                batch
                    .asSequence()
                    .filter { !it.additionalData.containsKey("@removed") }
                    .filter { u ->
                        val isMember = u.userType?.equals("member", ignoreCase = true) ?: false
                        if (!isMember) {
                            log.trace(
                                "Skipping non-member user from Microsoft Graph (correlationId={}, entraId={}, userType={})",
                                correlationId,
                                u.id,
                                u.userType,
                            )
                        }
                        isMember
                    }.mapNotNull { u ->
                        val id =
                            parseObjectIdOrNull(u.id)
                                ?: run {
                                    log.warn(
                                        "Skipping user with invalid Entra ID from Microsoft Graph (correlationId={}, entraId={})",
                                        correlationId,
                                        u.id,
                                    )
                                    return@mapNotNull null
                                }
                        id to u
                    }.distinctBy { it.first }
                    .toList()

            if (candidates.isEmpty()) return@coroutineScope 0

            val externals = ArrayList<Pair<UUID, User>>()
            val normals = ArrayList<Pair<UUID, User>>()

            for ((id, u) in candidates) {
                if (isExternal(u)) {
                    log.trace("Classified user as external (correlationId={}, entraId={})", correlationId, id)
                    externals += id to u
                } else {
                    log.trace("Classified user as internal (correlationId={}, entraId={})", correlationId, id)
                    normals += id to u
                }
            }
            if (republishAll) {
                val publishedUsers =
                    upsertAndPublishAll(
                        now = now,
                        repo = userRepository,
                        candidates = normals,
                        toDto = { u -> EntraUser(u, configUser) },
                        publish = { dto -> producer.publish(dto) },
                        checksum = { dto -> checksumService.checksum(dto) },
                        logLabel = "users",
                        correlationId = correlationId,
                    )

                val publishedExternal =
                    upsertAndPublishAll(
                        now = now,
                        repo = userExternalRepository,
                        candidates = externals,
                        toDto = { u -> EntraUserExternal(u, configUser) },
                        publish = { dto -> externalProducer.publish(dto) },
                        checksum = { dto -> checksumService.checksum(dto) },
                        logLabel = "external users",
                        correlationId = correlationId,
                    )
                publishedUsers + publishedExternal
            } else {
                val publishedUsers =
                    upsertAndPublishChanged(
                        now = now,
                        repo = userRepository,
                        candidates = normals,
                        toDto = { u -> EntraUser(u, configUser) },
                        publish = { dto -> producer.publish(dto) },
                        checksum = { dto -> checksumService.checksum(dto) },
                        logLabel = "users",
                        correlationId = correlationId,
                    )

                val publishedExternal =
                    upsertAndPublishChanged(
                        now = now,
                        repo = userExternalRepository,
                        candidates = externals,
                        toDto = { u -> EntraUserExternal(u, configUser) },
                        publish = { dto -> externalProducer.publish(dto) },
                        checksum = { dto -> checksumService.checksum(dto) },
                        logLabel = "external users",
                        correlationId = correlationId,
                    )
                publishedUsers + publishedExternal
            }
        }

    private suspend fun <DTO : Any> upsertAndPublishChanged(
        now: Instant,
        repo: UserStateRepository,
        candidates: List<Pair<UUID, User>>,
        toDto: (User) -> DTO,
        checksum: (DTO) -> Checksum,
        publish: suspend (DTO) -> Unit,
        logLabel: String,
        correlationId: String?,
    ): Int =
        coroutineScope {
            if (candidates.isEmpty()) return@coroutineScope 0

            val prepared =
                prepareRowsAndDtos(
                    now = now,
                    candidates = candidates,
                    toDto = toDto,
                    checksum = checksum,
                    correlationId = correlationId,
                    logLabel = logLabel,
                )

            val changedIds: Set<UUID> =
                dbBatchPermits.withPermit {
                    withContext(Dispatchers.IO) {
                        repo.batchUpsertReturningChanged(prepared.rows)
                    }
                }

            if (changedIds.isNotEmpty()) {
                log.debug("There are {} changed {}", changedIds.size, logLabel)
            }

            val jobs =
                changedIds.mapNotNull { id ->
                    val dto = prepared.dtoById[id] ?: return@mapNotNull null
                    async(Dispatchers.IO) {
                        publishWithLogging(
                            action = "changed",
                            logLabel = logLabel,
                            correlationId = correlationId,
                            entraId = id,
                            dto = dto,
                            publish = publish,
                        )
                    }
                }

            jobs.awaitAll().count { it.isSuccess }
        }

    private suspend fun <DTO : Any> upsertAndPublishAll(
        now: Instant,
        repo: UserStateRepository,
        candidates: List<Pair<UUID, User>>,
        toDto: (User) -> DTO,
        checksum: (DTO) -> Checksum,
        publish: suspend (DTO) -> Unit,
        logLabel: String,
        correlationId: String?,
    ): Int =
        coroutineScope {
            if (candidates.isEmpty()) return@coroutineScope 0

            val prepared =
                prepareRowsAndDtos(
                    now = now,
                    candidates = candidates,
                    toDto = toDto,
                    checksum = checksum,
                    correlationId = correlationId,
                    logLabel = logLabel,
                )

            dbBatchPermits.withPermit {
                withContext(Dispatchers.IO) {
                    repo.batchUpsert(prepared.rows)
                }
            }
            val jobs =
                prepared.rows.mapNotNull { row ->
                    val dto = prepared.dtoById[row.objectId] ?: return@mapNotNull null
                    async(Dispatchers.IO) {
                        publishWithLogging(
                            action = "republishAll",
                            logLabel = logLabel,
                            correlationId = correlationId,
                            entraId = row.objectId,
                            dto = dto,
                            publish = publish,
                        )
                    }
                }

            jobs.awaitAll().count { it.isSuccess }
        }

    private suspend fun handleRemoved(
        userId: String?,
        notSeenIncremented: MutableSet<UUID>,
    ) {
        if (userId.isNullOrBlank()) return
        val objectId =
            parseObjectIdOrNull(userId)
                ?: run {
                    log.warn("Skipping removed user with invalid Entra ID (entraId={})", userId)
                    return
                }

        if (!notSeenIncremented.add(objectId)) {
            log.debug("Removed user {} already marked not seen in this run; skipping", objectId)
            return
        }

        val existsInUsers =
            runCatching { withContext(Dispatchers.IO) { userRepository.existsById(objectId) } }
                .getOrDefault(false)

        val existsInExternal =
            runCatching { withContext(Dispatchers.IO) { userExternalRepository.existsById(objectId) } }
                .getOrDefault(false)

        when {
            existsInUsers -> {
                withContext(Dispatchers.IO) { userRepository.incrementNotSeenCount(listOf(objectId)) }
                log.debug("Marked user {} as not seen (+1) in users due to @removed", objectId)
            }

            existsInExternal -> {
                withContext(Dispatchers.IO) { userExternalRepository.incrementNotSeenCount(listOf(objectId)) }
                log.debug("Marked user {} as not seen (+1) in users_external due to @removed", objectId)
            }

            else -> {
                log.debug("Removed user {} not found in DB; skipping", objectId)
            }
        }
    }

    private suspend fun <DTO : Any> prepareRowsAndDtos(
        now: Instant,
        candidates: List<Pair<UUID, User>>,
        toDto: (User) -> DTO,
        checksum: (DTO) -> Checksum,
        correlationId: String?,
        logLabel: String,
    ): PreparedBatch<DTO> =
        coroutineScope {
            data class Computed<DTO>(
                val id: UUID,
                val dto: DTO,
                val checksum: Checksum,
            )

            val computed: List<Computed<DTO>> =
                candidates
                    .map { (id, u) ->
                        async(Dispatchers.Default) {
                            runCatching {
                                log.trace(
                                    "Preparing {} for upsert (correlationId={}, entraId={})",
                                    logLabel,
                                    correlationId,
                                    id,
                                )
                                val dto = toDto(u)
                                checksumPermits.withPermit {
                                    Computed(id, dto, checksum(dto))
                                }
                            }.onSuccess {
                                log.trace(
                                    "Prepared {} for upsert (correlationId={}, entraId={})",
                                    logLabel,
                                    correlationId,
                                    id,
                                )
                            }.onFailure {
                                log.warn(
                                    "Failed preparing {} for upsert (correlationId={}, entraId={})",
                                    logLabel,
                                    correlationId,
                                    id,
                                    it,
                                )
                            }.getOrThrow()
                        }
                    }.awaitAll()

            val rows = ArrayList<UserStateRepository.UpsertRow>(computed.size)
            val dtoById = HashMap<UUID, DTO>(computed.size)

            for (c in computed) {
                rows += UserStateRepository.UpsertRow(c.id, c.checksum, now)
                dtoById[c.id] = c.dto
                log.trace("Prepared DB upsert row for {} (correlationId={}, entraId={})", logLabel, correlationId, c.id)
            }

            PreparedBatch(rows = rows, dtoById = dtoById)
        }

    private suspend fun <DTO : Any> publishWithLogging(
        action: String,
        logLabel: String,
        correlationId: String?,
        entraId: UUID,
        dto: DTO,
        publish: suspend (DTO) -> Unit,
    ): Result<Unit> =
        runCatching {
            log.trace(
                "Publishing {} {} (correlationId={}, entraId={})",
                action,
                logLabel,
                correlationId,
                entraId,
            )
            kafkaPermits.withPermit {
                publish(dto)
            }
        }.onSuccess {
            log.trace(
                "Published {} {} (correlationId={}, entraId={})",
                action,
                logLabel,
                correlationId,
                entraId,
            )
        }.onFailure {
            log.warn(
                "Failed publishing {} {} (correlationId={}, entraId={})",
                action,
                logLabel,
                correlationId,
                entraId,
                it,
            )
        }

    private data class PreparedBatch<DTO : Any>(
        val rows: List<UserStateRepository.UpsertRow>,
        val dtoById: Map<UUID, DTO>,
    )

    private fun parseObjectIdOrNull(userId: String?): UUID? =
        if (userId.isNullOrBlank()) null else runCatching { UUID.fromString(userId) }.getOrNull()

    private fun isExternal(user: User): Boolean {
        if (configUser.enableExternalUsers != true) return false
        val attr = EntraUser.getAttributeValue(user, configUser.externaluserattribute) ?: return false
        val expected = configUser.externaluservalue ?: return false
        return attr.equals(expected, ignoreCase = true)
    }

    companion object {
        private val log = LoggerFactory.getLogger(EntraUserSyncService::class.java)
    }
}
