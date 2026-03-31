@file:Suppress("ktlint:standard:no-wildcard-imports")

package no.novari.msgraphgateway.device

import com.microsoft.graph.devices.delta.DeltaGetResponse
import com.microsoft.graph.serviceclient.GraphServiceClient
import com.microsoft.kiota.ApiException
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import no.novari.msgraphgateway.config.ConfigDevice
import no.novari.msgraphgateway.entra.DeltaLinkStore
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.concurrent.CompletionException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

@Component
class MsGraphDevice(
    private val configDevice: ConfigDevice,
    private val graphServiceClient: GraphServiceClient,
    private val entraDeviceSyncService: EntraDeviceSyncService,
    private val deltaLinkStore: DeltaLinkStore,
    private val coreDeviceRepository: CoreDeviceRepository,
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val runMutex = Mutex()
    private val fullImportRequested = AtomicBoolean(false)

    @Volatile
    private var deviceDeltaLink: String? = null

    @PostConstruct
    fun loadDeltaLink() {
        deviceDeltaLink = deltaLinkStore.find("devices")
        if (!deviceDeltaLink.isNullOrBlank()) {
            log.info("Loaded persisted devices deltaLink from DB")
        } else {
            log.info("No persisted devices deltaLink found (first run)")
        }
    }

    @PreDestroy
    fun shutdown() {
        scope.cancel("MsGraphDevice shutting down")
    }

    @Scheduled(
        initialDelayString = "\${novari.scheduler.device.delta.initial-delay-ms}",
        fixedDelayString = "\${novari.scheduler.device.delta.fixed-delay-ms}",
    )
    fun pullAllDevicesDelta() {
        if (fullImportRequested.get()) {
            log.info("Full import pending; skipping device delta run")
            return
        }

        scope.launch {
            if (!runMutex.tryLock()) {
                log.info("Device sync already running; skipping delta run")
                return@launch
            }

            val startTime = System.currentTimeMillis()
            try {
                val selection = configDevice.deviceAttributesDelta()
                val deltaPresent = !deviceDeltaLink.isNullOrBlank()

                log.info(
                    "Starting devices delta pull from Microsoft Graph (deltaLinkPresent={}, pageSize={})",
                    deltaPresent,
                    configDevice.devicePagingSize,
                )

                fun buildInitialRequest(link: String?): DeltaGetResponse? =
                    if (!link.isNullOrBlank()) {
                        graphServiceClient
                            .devices()
                            .delta()
                            .withUrl(link)
                            .get()
                    } else {
                        graphServiceClient
                            .devices()
                            .delta()
                            .get { req ->
                                req.queryParameters?.apply {
                                    select = selection
                                }
                            }
                    }

                val firstPage =
                    try {
                        callGraph { buildInitialRequest(deviceDeltaLink) }
                    } catch (ae: ApiException) {
                        if (!deviceDeltaLink.isNullOrBlank() && ae.isInvalidDeltaState()) {
                            log.warn("Resetting device deltaLink and retrying fresh delta.")

                            deviceDeltaLink = null
                            withContext(Dispatchers.IO) {
                                deltaLinkStore.createOrUpdate("devices", "")
                            }

                            callGraph { buildInitialRequest(null) }
                        } else {
                            throw ae
                        }
                    }

                val notSeenIncremented = ConcurrentHashMap.newKeySet<UUID>()
                pageThroughDevices(firstPage, isFullImport = false, notSeenIncremented = notSeenIncremented)
            } catch (e: RuntimeException) {
                log.error("Delta devices pull failed: {}", e.message, e)
            } finally {
                runMutex.unlock()
                logElapsed(startTime, "delta devices pull")
                tryStartFullImportIfRequested()
            }
        }
    }

    private fun ApiException.isInvalidDeltaState(): Boolean =
        responseStatusCode == 400 || responseStatusCode == 404 || responseStatusCode == 410

    @Scheduled(cron = "\${novari.scheduler.device.full-import.cron}")
    fun fullImportDevices() {
        fullImportRequested.set(true)

        scope.launch {
            if (!runMutex.tryLock()) {
                log.info("A device sync is running; full import requested and will start afterward")
                return@launch
            }

            val startTime = System.currentTimeMillis()
            try {
                startFullImport()
            } finally {
                runMutex.unlock()
                logElapsed(startTime, "full import of devices")
                fullImportRequested.set(false)
            }
        }
    }

    private suspend fun tryStartFullImportIfRequested() {
        if (!fullImportRequested.get()) return
        if (!runMutex.tryLock()) return

        val startTime = System.currentTimeMillis()
        try {
            startFullImport()
        } finally {
            runMutex.unlock()
            logElapsed(startTime, "full import of devices")
            fullImportRequested.set(false)
        }
    }

    private suspend fun startFullImport() {
        val runStartTime = Instant.now()
        val notSeenIncremented = ConcurrentHashMap.newKeySet<UUID>()

        if (!shouldContinueWithImport()) {
            return
        }

        val selection = configDevice.deviceAttributesDelta()

        log.info(
            "Starting full import of devices from Microsoft Graph (pageSize={})",
            configDevice.devicePagingSize,
        )

        val firstPage =
            callGraph {
                graphServiceClient
                    .devices()
                    .delta()
                    .get { req ->
                        req.headers.add("ConsistencyLevel", "eventual")
                        req.queryParameters?.select = selection
                    }
            }

        val result = pageThroughDevices(firstPage, isFullImport = true, notSeenIncremented = notSeenIncremented)
        markNotSeenDevices(runStartTime, notSeenIncremented)

        val cutoff = Instant.now().minus(configDevice.staleAfterDays.toLong(), ChronoUnit.DAYS)
        val deletedDevices =
            withContext(Dispatchers.IO) {
                entraDeviceSyncService.finishFullImport(cutoff)
            }

        log.info(
            "Full import completed (fetchedTotal={}, publishedChanged={}, publishedDeleted={})",
            result.totalDevicesSeen,
            result.publishedDevices,
            deletedDevices,
        )
    }

    private suspend fun markNotSeenDevices(
        startTime: Instant,
        notSeenIncremented: MutableSet<UUID>,
    ) {
        val staleDeviceIds =
            withContext(Dispatchers.IO) {
                coreDeviceRepository.findStaleObjectIds(startTime)
            }.filter { notSeenIncremented.add(it) }

        log.info("Marking {} stale devices as not seen", staleDeviceIds.size)

        withContext(Dispatchers.IO) {
            coreDeviceRepository.incrementNotSeenCount(staleDeviceIds)
        }
    }

    private fun shouldContinueWithImport(): Boolean {
        val totalCountSource = graphServiceClient.devices().count().get { req -> req.headers.add("ConsistencyLevel", "eventual") } ?: 0
        val totalCountDb = coreDeviceRepository.getCount()

        if (totalCountDb == 0) {
            log.info("Starting device import, DB is empty")
            return true
        }

        val deviation = kotlin.math.abs(totalCountSource - totalCountDb).toDouble() / totalCountDb.toDouble()
        val acceptedDeviation = (configDevice.acceptedDeviationPercent ?: 0).toDouble() / 100.0

        if (deviation > acceptedDeviation) {
            log.warn(
                "Skipping device import due to deviation check (sourceCount={}, dbCount={}, deviation={}, accepted={})",
                totalCountSource,
                totalCountDb,
                deviation,
                acceptedDeviation,
            )
            return false
        }

        log.info(
            "Starting device import (sourceCount={}, dbCount={}, deviation={}, accepted={})",
            totalCountSource,
            totalCountDb,
            deviation,
            acceptedDeviation,
        )
        return true
    }

    private suspend fun pageThroughDevices(
        firstPage: DeltaGetResponse?,
        isFullImport: Boolean,
        notSeenIncremented: MutableSet<UUID>,
    ): PageResult {
        var current: DeltaGetResponse? = firstPage
        var last: DeltaGetResponse? = firstPage

        var totalDevicesFetched = 0
        var totalPublished = 0
        var pageNo = 0

        val seenNextLinks = HashSet<String>()

        while (current != null) {
            pageNo++
            val value = current.value
            val fetchedThisPage = value?.size ?: 0

            if (fetchedThisPage > 0) {
                totalDevicesFetched += fetchedThisPage
                log.debug(
                    "Devices page {} fetched={} (fetchedTotalSoFar={})",
                    pageNo,
                    fetchedThisPage,
                    totalDevicesFetched,
                )

                val publishedThisPage =
                    withContext(Dispatchers.IO) {
                        entraDeviceSyncService.processPage(value, notSeenIncremented)
                    }

                totalPublished += publishedThisPage
            } else {
                log.debug("Devices page {} fetched=0", pageNo)
                log.trace(current.toString())
            }

            last = current

            val next = current.odataNextLink
            if (next.isNullOrBlank()) {
                current = null
            } else {
                if (!seenNextLinks.add(next)) {
                    log.error("Detected nextLink cycle; stopping paging (nextLink={})", next)
                    current = null
                } else {
                    current =
                        callGraph {
                            graphServiceClient
                                .devices()
                                .delta()
                                .withUrl(next)
                                .get()
                        }
                }
            }
        }

        val newDelta = last?.odataDeltaLink
        if (newDelta.isNullOrBlank()) {
            log.error("Last device page does not contain @odata.deltaLink; deltaLink not updated")
        } else {
            val initialRun = deviceDeltaLink.isNullOrEmpty()
            deviceDeltaLink = newDelta

            withContext(Dispatchers.IO) {
                deltaLinkStore.createOrUpdate("devices", newDelta)
            }

            if (!isFullImport) {
                log.info(
                    "Delta devices pull complete (initialRun={}, fetchedTotal={}, publishedChanged={})",
                    initialRun,
                    totalDevicesFetched,
                    totalPublished,
                )
            } else {
                log.info("Stored new deltaLink after full import")
            }
        }

        return PageResult(totalDevicesFetched, totalPublished)
    }

    private suspend fun <T> callGraph(block: () -> T): T =
        try {
            withContext(Dispatchers.IO) { block() }
        } catch (ae: ApiException) {
            log.error("Graph call failed with error code {}. {}", ae.responseStatusCode, ae.message)
            throw ae
        } catch (e: Exception) {
            throw if (e is RuntimeException) e else CompletionException(e)
        }

    private fun logElapsed(
        startTimeMs: Long,
        operation: String,
    ) {
        val elapsed = System.currentTimeMillis() - startTimeMs
        val minutes = (elapsed / 1000) / 60
        val seconds = (elapsed / 1000) % 60
        log.info("Finished {} in {}m {}s", operation, minutes, seconds)
    }

    private data class PageResult(
        val totalDevicesSeen: Int,
        val publishedDevices: Int,
    )

    companion object {
        private val log = LoggerFactory.getLogger(MsGraphDevice::class.java)
    }
}
