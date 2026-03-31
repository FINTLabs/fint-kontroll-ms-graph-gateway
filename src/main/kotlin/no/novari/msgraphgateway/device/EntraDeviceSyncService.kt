@file:Suppress("ktlint:standard:no-wildcard-imports")

package no.novari.msgraphgateway.device

import com.microsoft.graph.models.Device
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import no.novari.msgraphgateway.config.ConfigDevice
import no.novari.msgraphgateway.entra.ChecksumService
import no.novari.msgraphgateway.entra.EntraDevice
import no.novari.msgraphgateway.kafka.EntraDeviceProducerService
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Instant
import java.util.*

@Service
class EntraDeviceSyncService(
    private val coreDeviceRepository: CoreDeviceRepository,
    private val checksumService: ChecksumService,
    private val producer: EntraDeviceProducerService,
    private val configDevice: ConfigDevice,
) {
    private val batchSize = 1000
    private val checksumPermits = Semaphore(32)
    private val dbBatchPermits = Semaphore(2)
    private val kafkaPermits = Semaphore(100)

    suspend fun processPage(
        devices: List<Device>?,
        notSeenIncremented: MutableSet<UUID>,
    ): Int {
        if (devices.isNullOrEmpty()) return 0
        var publishedTotal = 0
        for (batch in devices.chunked(batchSize)) {
            publishedTotal += processBatch(batch, notSeenIncremented)
        }
        return publishedTotal
    }

    suspend fun finishFullImport(cutoff: Instant): Int =
        finishFullImportFor(
            repo = coreDeviceRepository,
            cutoff = cutoff,
            publishDeleted = { id -> producer.publishDeletedDevice(id) },
            label = "devices",
        )

    private suspend fun finishFullImportFor(
        repo: DeviceStateRepository,
        cutoff: Instant,
        publishDeleted: suspend (String) -> Unit,
        label: String,
    ): Int {
        val deletableIds =
            withContext(Dispatchers.IO) {
                repo.findStaleObjectIdsWithNotSeenCountGreaterThan(cutoff, configDevice.minNotSeenCount)
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
        batch: List<Device>,
        notSeenIncremented: MutableSet<UUID>,
    ): Int =
        coroutineScope {
            val now = Instant.now()

            val removedDevices = batch.filter { it.additionalData.containsKey("@removed") }
            if (removedDevices.isNotEmpty()) {
                log.debug("There are {} removed devices", removedDevices.size)
                removedDevices.forEach { d ->
                    handleRemoved(d.id, notSeenIncremented)
                }
            }

            val candidates: List<Pair<UUID, Device>> =
                batch
                    .asSequence()
                    .filter { !it.additionalData.containsKey("@removed") }
                    .mapNotNull { d ->
                        val id = parseObjectIdOrNull(d.id) ?: return@mapNotNull null
                        id to d
                    }.toList()

            if (candidates.isEmpty()) return@coroutineScope 0

            upsertAndPublish(
                now = now,
                repo = coreDeviceRepository,
                candidates = candidates,
                toDto = { d -> EntraDevice(d, configDevice) },
                publish = { dto -> producer.publish(dto) },
                checksum = { dto -> checksumService.checksum(dto) },
                logLabel = "devices",
            )
        }

    private suspend fun <DTO : Any> upsertAndPublish(
        now: Instant,
        repo: DeviceStateRepository,
        candidates: List<Pair<UUID, Device>>,
        toDto: (Device) -> DTO,
        checksum: (DTO) -> ByteArray,
        publish: suspend (DTO) -> Unit,
        logLabel: String,
    ): Int =
        coroutineScope {
            if (candidates.isEmpty()) return@coroutineScope 0

            data class Computed<DTO>(
                val id: UUID,
                val dto: DTO,
                val checksum: ByteArray,
            )

            val computed: List<Computed<DTO>> =
                candidates
                    .map { (id, d) ->
                        async(Dispatchers.Default) {
                            val dto = toDto(d)
                            checksumPermits.withPermit {
                                Computed(id, dto, checksum(dto))
                            }
                        }
                    }.awaitAll()

            val rows = ArrayList<DeviceStateRepository.UpsertRow>(computed.size)
            val dtoById = HashMap<UUID, DTO>(computed.size)

            for (c in computed) {
                rows += DeviceStateRepository.UpsertRow(c.id, c.checksum, now)
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
        deviceId: String?,
        notSeenIncremented: MutableSet<UUID>,
    ) {
        if (deviceId.isNullOrBlank()) return
        val objectId = parseObjectIdOrNull(deviceId) ?: return

        if (!notSeenIncremented.add(objectId)) {
            log.debug("Removed device {} already marked not seen in this run; skipping", objectId)
            return
        }

        val existsInDevices =
            runCatching { withContext(Dispatchers.IO) { coreDeviceRepository.existsById(objectId) } }
                .getOrDefault(false)

        if (existsInDevices) {
            withContext(Dispatchers.IO) { coreDeviceRepository.incrementNotSeenCount(listOf(objectId)) }
            log.debug("Marked device {} as not seen (+1) in devices due to @removed", objectId)
        } else {
            log.debug("Removed device {} not found in DB; skipping", objectId)
        }
    }

    private fun parseObjectIdOrNull(deviceId: String?): UUID? =
        if (deviceId.isNullOrBlank()) null else runCatching { UUID.fromString(deviceId) }.getOrNull()

    companion object {
        private val log = LoggerFactory.getLogger(EntraDeviceSyncService::class.java)
    }
}
