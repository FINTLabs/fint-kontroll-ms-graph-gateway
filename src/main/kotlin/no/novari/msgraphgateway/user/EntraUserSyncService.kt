package no.novari.msgraphgateway.user

import com.microsoft.graph.models.User
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import no.novari.msgraphgateway.azure.ChecksumService
import no.novari.msgraphgateway.azure.EntraUser
import no.novari.msgraphgateway.azure.EntraUserProducerService
import no.novari.msgraphgateway.config.ConfigUser
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Instant
import java.util.*

@Service
class EntraUserSyncService(
    private val coreUserRepository: CoreUserRepository,
    private val checksumService: ChecksumService,
    private val producer: EntraUserProducerService,
    private val configUser: ConfigUser,
) {
    /**
     * Parameters for tuning concurrency:
     * - batchSize: how many users to send to the DB in each batch
     * - checksumPermits: how many checksums can be computed in parallel
     * - dbBatchPermits: how many DB calls can be made in parallel
     * - kafkaPermits: how many messages can be published in parallel
     */

    private val batchSize = 1000
    private val checksumPermits = Semaphore(32)
    private val dbBatchPermits = Semaphore(2)
    private val kafkaPermits = Semaphore(100)

    suspend fun processPage(users: List<User>?): Int {
        if (users.isNullOrEmpty()) return 0

        var publishedTotal = 0
        for (batch in users.chunked(batchSize)) {
            val published = processBatch(batch)
            publishedTotal += published
        }
        return publishedTotal
    }

    suspend fun finishFullImport(cutoff: Instant): Int {
        val deletableIds =
            withContext(Dispatchers.IO) {
                coreUserRepository.findStaleObjectIdsWithNotSeenCountGreaterThan(cutoff, configUser.minNotSeenCount)
            }
        log.info("Found {} stale users", deletableIds.size)
        if (deletableIds.isEmpty()) return 0

        var deletedTotal = 0

        for (batch in deletableIds.chunked(batchSize)) {
            val deletedObjectIds =
                withContext(Dispatchers.IO) {
                    coreUserRepository.deleteByIdsReturningObjectIds(batch)
                }
            deletedTotal += deletedObjectIds.size

            coroutineScope {
                deletedObjectIds
                    .map { objectId ->
                        async(Dispatchers.IO) {
                            kafkaPermits.withPermit {
                                producer.publishDeletedUser(objectId.toString())
                            }
                        }
                    }.awaitAll()
            }
        }

        return deletedTotal
    }

    private suspend fun processBatch(batch: List<User>): Int =
        coroutineScope {
            val now = Instant.now()

            val removedUsers = batch.filter { it.additionalData.containsKey("@removed") }
            removedUsers.size

            for (u in removedUsers) {
                handleRemoved(u.id)
            }

            val activePairs: List<Pair<UUID, EntraUser>> =
                batch
                    .asSequence()
                    .filter { !it.additionalData.containsKey("@removed") }
                    .filter { u ->
                        return@filter u.userType?.equals("member", ignoreCase = true) ?: false
                    }.mapNotNull { u ->
                        val id = parseObjectIdOrNull(u.id)
                        if (id == null) {
                            return@mapNotNull null
                        }
                        val dto = EntraUser(u, configUser)
                        return@mapNotNull id to dto
                    }.toList()

            if (activePairs.isEmpty()) {
                return@coroutineScope 0
            }

            data class Computed(
                val id: UUID,
                val dto: EntraUser,
                val checksum: ByteArray,
            )

            val computed: List<Computed> =
                activePairs
                    .map { (id, dto) ->
                        async(Dispatchers.Default) {
                            checksumPermits.withPermit {
                                Computed(id, dto, checksumService.checksum(dto))
                            }
                        }
                    }.awaitAll()

            val rows = ArrayList<CoreUserRepository.UpsertRow>(computed.size)
            val dtoById = HashMap<UUID, EntraUser>(computed.size)

            for (c in computed) {
                rows +=
                    CoreUserRepository.UpsertRow(
                        objectId = c.id,
                        checksum = c.checksum,
                        lastSeenAt = now,
                    )
                dtoById[c.id] = c.dto
            }

            val changedIds: Set<UUID> =
                dbBatchPermits.withPermit {
                    withContext(Dispatchers.IO) {
                        coreUserRepository.batchUpsertReturningChanged(rows)
                    }
                }

            val publishJobs =
                changedIds.mapNotNull { id ->
                    val dto = dtoById[id] ?: return@mapNotNull null
                    if (isExternal(dto)) {
                        return@mapNotNull null
                        // publish as external user if it has the property
                    }

                    async(Dispatchers.IO) {
                        kafkaPermits.withPermit {
                            producer.publish(dto)
                        }
                        true
                    }
                }

            val publishedCount = publishJobs.awaitAll().count { it }
            return@coroutineScope publishedCount
        }

    private suspend fun handleRemoved(userId: String?) {
        if (userId.isNullOrBlank()) return

        val objectId = parseObjectIdOrNull(userId)
        if (objectId != null) {
            try {
                withContext(Dispatchers.IO) {
                    coreUserRepository.deleteById(objectId)
                }
            } catch (e: Exception) {
                log.warn("Failed deleting removed user {} from DB", objectId, e)
            }
        }

        try {
            withContext(Dispatchers.IO) {
                kafkaPermits.withPermit {
                    producer.publishDeletedUser(userId)
                }
            }
        } catch (e: Exception) {
            log.warn("Failed publishing tombstone for removed userId={}", userId, e)
        }
    }

    private fun parseObjectIdOrNull(userId: String?): UUID? {
        if (userId.isNullOrBlank()) return null
        return try {
            UUID.fromString(userId)
        } catch (_: Exception) {
            return null
        }
    }

    private fun isExternal(dto: EntraUser): Boolean {
        val employeeId = dto.employeeId
        val studentId = dto.studentId
        return employeeId.isNullOrEmpty() && studentId.isNullOrEmpty()
    }

    companion object {
        private val log = LoggerFactory.getLogger(EntraUserSyncService::class.java)
    }
}
