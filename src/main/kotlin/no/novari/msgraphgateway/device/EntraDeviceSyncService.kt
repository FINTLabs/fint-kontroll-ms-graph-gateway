@file:Suppress("ktlint:standard:no-wildcard-imports")

package no.novari.msgraphgateway.device

import com.microsoft.graph.models.Device
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import no.novari.msgraphgateway.config.ConfigDevice
import no.novari.msgraphgateway.entra.EntraDevice
import no.novari.msgraphgateway.kafka.EntraDeviceProducerService
import no.novari.msgraphgateway.service.Checksum
import no.novari.msgraphgateway.service.ChecksumService
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Instant
import java.util.*

@Service
class EntraDeviceSyncService(
    private val deviceRepository: DeviceStateRepository,
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
        republishAll: Boolean,
    ): Int {
        if (devices.isNullOrEmpty()) return 0

        var publishedTotal = 0
        for (batch in devices.chunked(batchSize)) {
            publishedTotal += processBatch(batch, notSeenIncremented, republishAll)
        }
        return publishedTotal
    }

    suspend fun finishFullImport(cutoff: Instant): Int =
        finishFullImportFor(
            repo = deviceRepository,
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
        republishAll: Boolean,
    ): Int =
        coroutineScope {
            val now = Instant.now()

            val removedDevices = batch.filter { it.additionalData.containsKey("@removed") }
            if (removedDevices.isNotEmpty()) {
                log.info("There are {} removed devices", removedDevices.size)
                removedDevices.forEach { device ->
                    handleRemoved(device.id, notSeenIncremented)
                }
            }

            val candidates: List<Pair<UUID, Device>> =
                batch
                    .asSequence()
                    .filter { !it.additionalData.containsKey("@removed") }
                    .mapNotNull { device ->
                        val id = parseObjectIdOrNull(device.id) ?: return@mapNotNull null
                        id to device
                    }.distinctBy { it.first }
                    .toList()

            if (candidates.isEmpty()) return@coroutineScope 0

            if (republishAll) {
                upsertAndPublishAll(
                    now = now,
                    repo = deviceRepository,
                    candidates = candidates,
                    toDto = { device -> EntraDevice(device, configDevice) },
                    publish = { dto -> producer.publish(dto) },
                    checksum = { dto -> checksumService.checksum(dto) },
                    logLabel = "devices",
                )
            } else {
                upsertAndPublishChanged(
                    now = now,
                    repo = deviceRepository,
                    candidates = candidates,
                    toDto = { device -> EntraDevice(device, configDevice) },
                    publish = { dto -> producer.publish(dto) },
                    checksum = { dto -> checksumService.checksum(dto) },
                    logLabel = "devices",
                )
            }
        }

    private suspend fun <DTO : Any> upsertAndPublishChanged(
        now: Instant,
        repo: DeviceStateRepository,
        candidates: List<Pair<UUID, Device>>,
        toDto: (Device) -> DTO,
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
                        runCatching {
                            kafkaPermits.withPermit {
                                publish(dto)
                            }
                        }.onFailure { log.warn("Failed publishing {} {}", logLabel, id, it) }
                    }
                }

            jobs.awaitAll().count { it.isSuccess }
        }

    private suspend fun <DTO : Any> upsertAndPublishAll(
        now: Instant,
        repo: DeviceStateRepository,
        candidates: List<Pair<UUID, Device>>,
        toDto: (Device) -> DTO,
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
                    repo.batchUpsert(prepared.rows)
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
                        }.onFailure { log.warn("Failed publishing {} {}", logLabel, row.objectId, it) }
                    }
                }

            jobs.awaitAll().count { it.isSuccess }
        }

    private suspend fun <DTO : Any> prepareRowsAndDtos(
        now: Instant,
        candidates: List<Pair<UUID, Device>>,
        toDto: (Device) -> DTO,
        checksum: (DTO) -> Checksum,
    ): PreparedBatch<DTO> =
        coroutineScope {
            data class Computed<DTO>(
                val id: UUID,
                val dto: DTO,
                val checksum: Checksum,
            )

            val computed: List<Computed<DTO>> =
                candidates
                    .map { (id, device) ->
                        async(Dispatchers.Default) {
                            val dto = toDto(device)
                            checksumPermits.withPermit {
                                Computed(id, dto, checksum(dto))
                            }
                        }
                    }.awaitAll()

            val rows = ArrayList<DeviceStateRepository.UpsertRow>(computed.size)
            val dtoById = HashMap<UUID, DTO>(computed.size)

            for (item in computed) {
                rows += DeviceStateRepository.UpsertRow(item.id, item.checksum, now)
                dtoById[item.id] = item.dto
            }

            PreparedBatch(rows = rows, dtoById = dtoById)
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

        val exists =
            runCatching {
                withContext(Dispatchers.IO) {
                    deviceRepository.existsById(objectId)
                }
            }.getOrDefault(false)

        if (exists) {
            withContext(Dispatchers.IO) {
                deviceRepository.incrementNotSeenCount(listOf(objectId))
            }
            log.debug("Marked device {} as not seen (+1) in devices due to @removed", objectId)
        } else {
            log.debug("Removed device {} not found in DB; skipping", objectId)
        }
    }

    private data class PreparedBatch<DTO : Any>(
        val rows: List<DeviceStateRepository.UpsertRow>,
        val dtoById: Map<UUID, DTO>,
    )

    private fun parseObjectIdOrNull(deviceId: String?): UUID? =
        if (deviceId.isNullOrBlank()) null else runCatching { UUID.fromString(deviceId) }.getOrNull()

    companion object {
        private val log = LoggerFactory.getLogger(EntraDeviceSyncService::class.java)
    }
}
