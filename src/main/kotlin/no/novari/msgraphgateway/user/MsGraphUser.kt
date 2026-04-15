package no.novari.msgraphgateway.user

import com.microsoft.graph.serviceclient.GraphServiceClient
import com.microsoft.graph.users.delta.DeltaGetResponse
import com.microsoft.kiota.ApiException
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import no.novari.msgraphgateway.config.ConfigUser
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
class MsGraphUser(
    private val configUser: ConfigUser,
    private val graphServiceClient: GraphServiceClient,
    private val entraUserSyncService: EntraUserSyncService,
    private val deltaLinkStore: DeltaLinkStore,
    private val userRepository: UserRepository,
    private val userExternalRepository: UserExternalRepository,
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val runMutex = Mutex()
    private val fullImportRequested = AtomicBoolean(false)
    private val republishAllRequested = AtomicBoolean(false)

    @Volatile
    private var userDeltaLink: String? = null

    @PostConstruct
    fun loadDeltaLink() {
        userDeltaLink = deltaLinkStore.find("users")
        if (!userDeltaLink.isNullOrBlank()) {
            log.info("Loaded persisted users deltaLink from DB")
        } else {
            log.info("No persisted users deltaLink found (first run)")
        }
    }

    @PreDestroy
    fun shutdown() {
        scope.cancel("MsGraphUser shutting down")
    }

    @Scheduled(
        initialDelayString = $$"${novari.scheduler.user.delta.initial-delay-ms}",
        fixedDelayString = $$"${novari.scheduler.user.delta.fixed-delay-ms}",
    )
    fun pullAllUsersDelta() {
        if (fullImportRequested.get()) {
            log.info("Full import pending; skipping delta run")
            return
        }

        scope.launch {
            if (!runMutex.tryLock()) {
                log.info("User sync already running; skipping delta run")
                return@launch
            }

            val startTime = System.currentTimeMillis()
            val trackingId = UUID.randomUUID().toString()
            try {
                val selection = configUser.userAttributesDelta()
                val deltaPresent = !userDeltaLink.isNullOrBlank()

                log.info("Starting users delta pull from Microsoft Graph")
                log.debug(
                    "TrackingId {} (deltaLinkPresent={}, pageSize={})",
                    trackingId,
                    deltaPresent,
                    configUser.userpagingsize,
                )

                fun buildInitialRequest(link: String?): DeltaGetResponse? =
                    if (!link.isNullOrBlank()) {
                        graphServiceClient
                            .users()
                            .delta()
                            .withUrl(link)
                            .get { req ->
                                req.headers.add("client-request-id", trackingId)
                            }
                    } else {
                        graphServiceClient
                            .users()
                            .delta()
                            .get { req ->
                                req.headers.add("client-request-id", trackingId)
                                req.queryParameters?.apply {
                                    select = selection
                                }
                            }
                    }

                val firstPage =
                    try {
                        callGraph { buildInitialRequest(userDeltaLink) }
                    } catch (ae: ApiException) {
                        if (!userDeltaLink.isNullOrBlank() && ae.isInvalidDeltaState()) {
                            log.warn("Resetting deltaLink and retrying fresh delta.")

                            userDeltaLink = null
                            withContext(Dispatchers.IO) {
                                deltaLinkStore.createOrUpdate("users", "")
                            }

                            callGraph { buildInitialRequest(null) }
                        } else {
                            throw ae
                        }
                    }

                val notSeenIncremented = ConcurrentHashMap.newKeySet<UUID>()
                pageThroughUsers(firstPage, isFullImport = false, notSeenIncremented = notSeenIncremented, false)
            } catch (e: RuntimeException) {
                log.error("Delta users pull failed: {}", e.message, e)
            } finally {
                runMutex.unlock()
                logElapsed(startTime, "delta users pull")
                tryStartFullImportIfRequested()
            }
        }
    }

    private fun ApiException.isInvalidDeltaState(): Boolean =
        responseStatusCode == 400 || responseStatusCode == 404 || responseStatusCode == 410

    @Scheduled(cron = $$"${novari.scheduler.user.full-import.cron}")
    fun fullImportUsers() {
        requestFullImport(false)
    }

    fun requestFullImport(republishAll: Boolean = false) {
        fullImportRequested.set(true)
        if (republishAll) {
            republishAllRequested.set(true)
        }

        scope.launch {
            if (!runMutex.tryLock()) {
                log.info("A sync is running; full import requested and will start afterward")
                return@launch
            }

            val startTime = System.currentTimeMillis()
            val republishRequested = republishAllRequested.getAndSet(false)

            try {
                startFullImport(republishRequested)
            } finally {
                runMutex.unlock()
                logElapsed(startTime, "full import of users")
                fullImportRequested.set(false)
            }
        }
    }

    private suspend fun tryStartFullImportIfRequested() {
        if (!fullImportRequested.get()) return
        if (!runMutex.tryLock()) return

        val startTime = System.currentTimeMillis()
        val republishRequested = republishAllRequested.getAndSet(false)
        try {
            startFullImport(republishRequested)
        } finally {
            runMutex.unlock()
            logElapsed(startTime, "full import of users")
            fullImportRequested.set(false)
        }
    }

    suspend fun startFullImport(republishAll: Boolean = false) {
        val runStartTime = Instant.now()
        val trackingId = UUID.randomUUID().toString()
        val notSeenIncremented = ConcurrentHashMap.newKeySet<UUID>()
        if (!shouldContinueWithImport()) {
            return
        }
        val selection = configUser.userAttributesDelta()

        log.info("Starting full import of users from Microsoft Graph")
        log.debug(
            "Starting full import of users from Microsoft Graph. TrackingID {} (pageSize={})",
            trackingId,
            configUser.userpagingsize,
        )

        val firstPage =
            callGraph {
                graphServiceClient
                    .users()
                    .delta()
                    .get { req ->
                        req.headers.add("client-request-id", trackingId)
                        req.queryParameters?.select = selection
                    }
            }

        val result =
            pageThroughUsers(firstPage, isFullImport = true, notSeenIncremented = notSeenIncremented, republishAll)
        markNotSeenUsers(runStartTime, notSeenIncremented)
        val cutoff = Instant.now().minus(configUser.staleAfterDays.toLong(), ChronoUnit.DAYS)
        val deletedUsers = withContext(Dispatchers.IO) { entraUserSyncService.finishFullImport(cutoff) }
        val deletedExternal = withContext(Dispatchers.IO) { entraUserSyncService.finishFullImportExternal(cutoff) }

        log.info(
            "Full import completed (fetchedTotal={}, publishedChanged={}, publishedDeleted={}, publishedDeletedExternal={})",
            result.totalUsersSeen,
            result.publishedUsers,
            deletedUsers,
            deletedExternal,
        )
    }

    private suspend fun markNotSeenUsers(
        startTime: Instant,
        notSeenIncremented: MutableSet<UUID>,
    ) {
        val staleUserIds =
            withContext(Dispatchers.IO) {
                userRepository.findStaleObjectIds(startTime)
            }.filter { notSeenIncremented.add(it) } // add() returnerer false hvis allerede der

        log.info("Marking {} stale users as not seen", staleUserIds.size)
        withContext(Dispatchers.IO) {
            userRepository.incrementNotSeenCount(staleUserIds)
        }

        val staleExternalIds =
            withContext(Dispatchers.IO) {
                userExternalRepository.findStaleObjectIds(startTime)
            }.filter { notSeenIncremented.add(it) }

        log.info("Marking {} stale external users as not seen", staleExternalIds.size)
        withContext(Dispatchers.IO) {
            userExternalRepository.incrementNotSeenCount(staleExternalIds)
        }
    }

    private fun shouldContinueWithImport(): Boolean {
        val totalCountSource =
            graphServiceClient
                .users()
                .count()
                .get { req ->
                    req.headers.add("ConsistencyLevel", "eventual")
                    req.queryParameters?.filter = "userType eq 'Member'"
                } ?: 0
        val totalCountDb = userRepository.getCount()
        if (totalCountDb != 0 &&
            Math.abs(totalCountSource - totalCountDb).div(totalCountDb) <
            Math.divideExact(
                configUser.acceptedDeviationPercent ?: 0,
                100,
            )
        ) {
            log.info("Not starting import, as the coverage is too low")
            return false
        }
        log.info("Starting import, fetched total count is $totalCountSource, db count is $totalCountDb")
        return true
    }

    private suspend fun pageThroughUsers(
        firstPage: DeltaGetResponse?,
        isFullImport: Boolean,
        notSeenIncremented: MutableSet<UUID>,
        republishAll: Boolean,
    ): PageResult {
        var current: DeltaGetResponse? = firstPage
        var last: DeltaGetResponse? = firstPage

        var totalUsersFetched = 0
        var totalPublished = 0
        var pageNo = 0

        val seenNextLinks = HashSet<String>()

        while (current != null) {
            pageNo++
            val value = current.value
            val fetchedThisPage = value?.size ?: 0

            if (fetchedThisPage > 0) {
                totalUsersFetched += fetchedThisPage
                log.debug(
                    "Users page {} fetched={} (fetchedTotalSoFar={})",
                    pageNo,
                    fetchedThisPage,
                    totalUsersFetched,
                )

                val publishedThisPage =
                    withContext(Dispatchers.IO) {
                        entraUserSyncService.processPage(value, notSeenIncremented, republishAll)
                    }
                totalPublished += publishedThisPage
            } else {
                log.debug("Users page {} fetched=0", pageNo)
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
                                .users()
                                .delta()
                                .withUrl(next)
                                .get()
                        }
                }
            }
        }

        val newDelta = last?.odataDeltaLink
        if (newDelta.isNullOrBlank()) {
            log.error("Last user page does not contain @odata.deltaLink; deltaLink not updated")
        } else {
            val initialRun = userDeltaLink.isNullOrEmpty()
            userDeltaLink = newDelta

            withContext(Dispatchers.IO) {
                deltaLinkStore.createOrUpdate("users", newDelta)
            }

            if (!isFullImport) {
                log.info(
                    "Delta users pull complete (initialRun={}, fetchedTotal={}, publishedChanged={})",
                    initialRun,
                    totalUsersFetched,
                    totalPublished,
                )
            } else {
                log.info("Stored new deltaLink after full import")
            }
        }

        return PageResult(totalUsersFetched, totalPublished)
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
        val totalUsersSeen: Int,
        val publishedUsers: Int,
    )

    companion object {
        private val log = LoggerFactory.getLogger(MsGraphUser::class.java)
    }
}
