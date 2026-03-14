package no.novari.msgraphgateway.user

import com.microsoft.graph.models.User
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import no.novari.msgraphgateway.config.ConfigUser
import no.novari.msgraphgateway.entra.Checksum
import no.novari.msgraphgateway.entra.ChecksumService
import no.novari.msgraphgateway.entra.EntraUser
import no.novari.msgraphgateway.entra.EntraUserExternal
import no.novari.msgraphgateway.kafka.EntraUserExternalProducerService
import no.novari.msgraphgateway.kafka.EntraUserProducerService
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Instant
import java.util.*

@Service
class EntraUserSyncService(
    private val userRepository: UserRepository,
    private val userExternalRepository: UserExternalRepository,
    private val checksumService: ChecksumService,
    private val producer: EntraUserProducerService,
    private val externalProducer: EntraUserExternalProducerService,
    private val configUser: ConfigUser,
) {
    private val batchSize = 1000
    private val checksumPermits = Semaphore(32)
    private val dbBatchPermits = Semaphore(2)
    private val kafkaPermits = Semaphore(100)

    suspend fun processPage(
        users: List<User>?,
        notSeenIncremented: MutableSet<UUID>,
    ): Int {
        if (users.isNullOrEmpty()) return 0
        var publishedTotal = 0
        for (batch in users.chunked(batchSize)) {
            publishedTotal += processBatch(batch, notSeenIncremented)
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
                                publishDeleted(objectId.toString())
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
    ): Int =
        coroutineScope {
            val now = Instant.now()

            val removedUsers = batch.filter { it.additionalData.containsKey("@removed") }
            if (removedUsers.isNotEmpty()) {
                log.debug("There are {} removed users", removedUsers.size)
                removedUsers.forEach { u ->
                    handleRemoved(u.id, notSeenIncremented)
                }
            }

            val candidates: List<Pair<UUID, User>> =
                batch
                    .asSequence()
                    .filter { !it.additionalData.containsKey("@removed") }
                    .filter { it.userType?.equals("member", ignoreCase = true) ?: false }
                    .mapNotNull { u ->
                        val id = parseObjectIdOrNull(u.id) ?: return@mapNotNull null
                        id to u
                    }.toList()

            if (candidates.isEmpty()) return@coroutineScope 0

            val externals = ArrayList<Pair<UUID, User>>()
            val normals = ArrayList<Pair<UUID, User>>()

            for ((id, u) in candidates) {
                if (isExternal(u)) externals += id to u else normals += id to u
            }

            val publishedUsers =
                upsertAndPublish(
                    now = now,
                    repo = userRepository,
                    candidates = normals,
                    toDto = { u -> EntraUser(u, configUser) },
                    publish = { dto -> producer.publish(dto) },
                    checksum = { dto -> checksumService.checksum(dto) },
                    logLabel = "users",
                )

            val publishedExternal =
                upsertAndPublish(
                    now = now,
                    repo = userExternalRepository,
                    candidates = externals,
                    toDto = { u -> EntraUserExternal(u, configUser) },
                    publish = { dto -> externalProducer.publish(dto) },
                    checksum = { dto -> checksumService.checksum(dto) },
                    logLabel = "external users",
                )

            publishedUsers + publishedExternal
        }

    private suspend fun <DTO : Any> upsertAndPublish(
        now: Instant,
        repo: UserStateRepository,
        candidates: List<Pair<UUID, User>>,
        toDto: (User) -> DTO,
        checksum: (DTO) -> Checksum,
        publish: suspend (DTO) -> Unit,
        logLabel: String,
    ): Int =
        coroutineScope {
            if (candidates.isEmpty()) return@coroutineScope 0

            data class Computed<DTO>(
                val id: UUID,
                val dto: DTO,
                val checksum: Checksum,
            )

            val computed: List<Computed<DTO>> =
                candidates
                    .map { (id, u) ->
                        async(Dispatchers.Default) {
                            val dto = toDto(u)
                            checksumPermits.withPermit {
                                Computed(id, dto, checksum(dto))
                            }
                        }
                    }.awaitAll()

            val rows = ArrayList<UserStateRepository.UpsertRow>(computed.size)
            val dtoById = HashMap<UUID, DTO>(computed.size)

            for (c in computed) {
                rows += UserStateRepository.UpsertRow(c.id, c.checksum, now)
                dtoById[c.id] = c.dto
            }

            val changedIds: Set<UUID> =
                dbBatchPermits.withPermit {
                    withContext(Dispatchers.IO) {
                        repo.batchUpsertReturningChanged(rows)
                    }
                }

            if (changedIds.isNotEmpty()) {
                log.debug("There are {} changed {}", changedIds.size, logLabel)
            }

            val jobs =
                changedIds.mapNotNull { id ->
                    val dto = dtoById[id] ?: return@mapNotNull null
                    async(Dispatchers.IO) {
                        runCatching {
                            kafkaPermits.withPermit {
                                publish(dto)
                            }
                        }.onFailure { log.warn("Failed publishing {} {}", logLabel, id, it) }
                    }
                }

            jobs.awaitAll().count { it.isSuccess }
        }

    private suspend fun handleRemoved(
        userId: String?,
        notSeenIncremented: MutableSet<UUID>,
    ) {
        if (userId.isNullOrBlank()) return
        val objectId = parseObjectIdOrNull(userId) ?: return

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
