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
import kotlin.math.abs

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
            val correlationId = UUID.randomUUID().toString()
            try {
                val selection = configUser.userAttributesDelta()
                val deltaPresent = !userDeltaLink.isNullOrBlank()

                log.info("Starting users delta pull from Microsoft Graph")
                log.debug(
                    "Starting users delta pull (correlationId={}, deltaLinkPresent={}, pageSize={})",
                    correlationId,
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
                                req.headers.add("client-request-id", correlationId)
                            }
                    } else {
                        graphServiceClient
                            .users()
                            .delta()
                            .get { req ->
                                req.headers.add("client-request-id", correlationId)
                                req.headers.add("ConsistencyLevel", "eventual")
                                req.queryParameters?.apply {
                                    select = selection
                                }
                            }
                    }

                val firstPage =
                    try {
                        callGraph("users delta first page", correlationId) { buildInitialRequest(userDeltaLink) }
                    } catch (exception: ApiException) {
                        if (!userDeltaLink.isNullOrBlank() && exception.isInvalidDeltaState()) {
                            log.warn("Resetting deltaLink and retrying fresh delta.")
                            log.debug(
                                "Invalid users deltaLink rejected by Microsoft Graph (correlationId={})",
                                correlationId,
                            )
                            log.trace("Invalid users deltaLink value: {}", userDeltaLink)

                            userDeltaLink = null
                            withContext(Dispatchers.IO) {
                                deltaLinkStore.createOrUpdate("users", "")
                            }

                            callGraph(
                                "users delta first page after delta reset",
                                correlationId,
                            ) {
                                buildInitialRequest(null)
                            }
                        } else {
                            throw exception
                        }
                    }

                val notSeenIncremented = ConcurrentHashMap.newKeySet<UUID>()
                pageThroughUsers(
                    firstPage = firstPage,
                    isFullImport = false,
                    notSeenIncremented = notSeenIncremented,
                    republishAll = false,
                    correlationId = correlationId,
                )
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

    @Scheduled(cron = $$"${novari.scheduler.user.weekly-publish.cron}")
    fun weeklyPublishUsers() {
        requestFullImport(true)
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
        val correlationId = UUID.randomUUID().toString()
        val notSeenIncremented = ConcurrentHashMap.newKeySet<UUID>()
        if (!shouldContinueWithImport()) {
            return
        }
        val selection = configUser.userAttributesDelta()

        log.info("Starting full import of users from Microsoft Graph")
        log.debug(
            "Starting full import of users from Microsoft Graph (correlationId={}, pageSize={}, republishAll={})",
            correlationId,
            configUser.userpagingsize,
            republishAll,
        )
        log.trace("Full import user select attributes: {}", selection.joinToString(","))

        val firstPage =
            callGraph("users full import first page", correlationId) {
                graphServiceClient
                    .users()
                    .delta()
                    .get { req ->
                        req.headers.add("client-request-id", correlationId)
                        req.headers.add("ConsistencyLevel", "eventual")
                        req.queryParameters?.select = selection
                    }
            }

        val result =
            pageThroughUsers(
                firstPage = firstPage,
                isFullImport = true,
                notSeenIncremented = notSeenIncremented,
                republishAll = republishAll,
                correlationId = correlationId,
            )
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
        val correlationId = UUID.randomUUID().toString()
        val totalCountSource =
            callGraphBlocking("users count", correlationId) {
                graphServiceClient
                    .users()
                    .count()
                    .get { req ->
                        req.headers.add("client-request-id", correlationId)
                        req.headers.add("ConsistencyLevel", "eventual")
                        req.queryParameters?.filter = "userType eq 'Member'"
                    } ?: 0
            }
        val totalCountDb = userRepository.getCount()
        log.debug(
            "User import coverage check (correlationId={}, sourceCount={}, dbCount={}, acceptedDeviationPercent={})",
            correlationId,
            totalCountSource,
            totalCountDb,
            configUser.acceptedDeviationPercent,
        )
        if (totalCountDb != 0 &&
            abs(totalCountSource - totalCountDb).div(totalCountDb) <
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
        correlationId: String,
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
                        entraUserSyncService.processPage(value, notSeenIncremented, republishAll, correlationId)
                    }
                totalPublished += publishedThisPage
            } else {
                log.debug("Users page {} fetched=0", pageNo)
                log.trace(
                    "Users page {} metadata (nextLinkPresent={}, deltaLinkPresent={})",
                    pageNo,
                    !current.odataNextLink.isNullOrBlank(),
                    !current.odataDeltaLink.isNullOrBlank(),
                )
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
                    log.trace("Fetching users page {} from nextLink={}", pageNo + 1, next)
                    current =
                        callGraph("users delta next page", correlationId) {
                            graphServiceClient
                                .users()
                                .delta()
                                .withUrl(next)
                                .get { req ->
                                    req.headers.add("client-request-id", correlationId)
                                }
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
            log.trace("Storing users deltaLink value: {}", newDelta)

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

    private suspend fun <T> callGraph(
        operation: String,
        correlationId: String? = null,
        block: () -> T,
    ): T {
        val startTime = System.currentTimeMillis()
        log.trace("Starting Microsoft Graph call: {} (correlationId={})", operation, correlationId)
        return try {
            withContext(Dispatchers.IO) { block() }.also {
                logGraphCallElapsed(operation, correlationId, startTime)
            }
        } catch (apiException: ApiException) {
            log.error(
                "Microsoft Graph call failed: {} (correlationId={}, status={}, elapsedMs={}). {}",
                operation,
                correlationId,
                apiException.responseStatusCode,
                System.currentTimeMillis() - startTime,
                apiException.message,
            )
            throw apiException
        } catch (exception: Exception) {
            throw exception as? RuntimeException ?: CompletionException(exception)
        }
    }

    private fun <T> callGraphBlocking(
        operation: String,
        correlationId: String? = null,
        block: () -> T,
    ): T {
        val startTime = System.currentTimeMillis()
        log.trace("Starting Microsoft Graph call: {} (correlationId={})", operation, correlationId)
        return try {
            block().also {
                logGraphCallElapsed(operation, correlationId, startTime)
            }
        } catch (apiException: ApiException) {
            log.error(
                "Microsoft Graph call failed: {} (correlationId={}, status={}, elapsedMs={}). {}",
                operation,
                correlationId,
                apiException.responseStatusCode,
                System.currentTimeMillis() - startTime,
                apiException.message,
            )
            throw apiException
        }
    }

    private fun logGraphCallElapsed(
        operation: String,
        correlationId: String?,
        startTimeMs: Long,
    ) {
        val elapsed = System.currentTimeMillis() - startTimeMs
        log.debug(
            "Microsoft Graph call completed: {} (correlationId={}, elapsedMs={})",
            operation,
            correlationId,
            elapsed,
        )
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
