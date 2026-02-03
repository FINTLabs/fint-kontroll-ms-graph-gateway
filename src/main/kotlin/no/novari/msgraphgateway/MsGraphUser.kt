package no.novari.msgraphgateway

import com.microsoft.graph.serviceclient.GraphServiceClient
import com.microsoft.graph.users.delta.DeltaGetResponse
import com.microsoft.kiota.ApiException
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import no.novari.msgraphgateway.azure.DeltaLinkStore
import no.novari.msgraphgateway.config.ConfigUser
import no.novari.msgraphgateway.user.CoreUserRepository
import no.novari.msgraphgateway.user.EntraUserSyncService
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.CompletionException
import java.util.concurrent.atomic.AtomicBoolean

@Component
class MsGraphUser(
    private val configUser: ConfigUser,
    private val graphServiceClient: GraphServiceClient,
    private val entraUserSyncService: EntraUserSyncService,
    private val deltaLinkStore: DeltaLinkStore,
    private val coreUserRepository: CoreUserRepository
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val runMutex = Mutex()
    private val fullImportRequested = AtomicBoolean(false)

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
        initialDelayString = "90000",
        fixedDelayString = "\${fint.kontroll.ms-graph-gateway.user-scheduler.delta.fixed-delay-ms}"
    )
    fun pullAllUsersDelta() {
        log.info("Config user")
        log.info(configUser.allAttributes().joinToString(", "))
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
            try {
                val selection = configUser.userAttributesDelta()
                val deltaPresent = !userDeltaLink.isNullOrBlank()

                log.info(
                    "Starting users delta pull from Microsoft Graph (deltaLinkPresent={}, pageSize={})",
                    deltaPresent, configUser.userpagingsize
                )

                val firstPage = callGraph {
                    val link = userDeltaLink
                    if (!link.isNullOrBlank()) {
                        graphServiceClient.users().delta().withUrl(link).get()
                    } else {
                        graphServiceClient.users()
                            .delta()
                            .get { req ->
                                req.queryParameters?.apply {
                                    select = selection
                                    top = configUser.userpagingsize
                                }
                            }
                    }
                }

                pageThroughUsers(firstPage, isFullImport = false)
            } catch (e: RuntimeException) {
                log.error("Delta users pull failed: {}", e.message, e)
            } finally {
                runMutex.unlock()
                logElapsed(startTime, "delta users pull")
                tryStartFullImportIfRequested()
            }
        }
    }

    @Scheduled(cron = "\${fint.kontroll.ms-graph-gateway.user-scheduler.full-import-cron:0 0 2 * * *}")
    fun fullImportUsers() {
        fullImportRequested.set(true)

        scope.launch {
            if (!runMutex.tryLock()) {
                log.info("A sync is running; full import requested and will start afterward")
                return@launch
            }

            val startTime = System.currentTimeMillis()
            try {
                startFullImport()
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
        try {
            startFullImport()
        } finally {
            runMutex.unlock()
            logElapsed(startTime, "full import of users")
            fullImportRequested.set(false)
        }
    }

    private suspend fun startFullImport() {
        val runStartTime = Instant.now()
        if (!shouldContinueWithImport())
            return
        val selection = configUser.userAttributesDelta()

        log.info(
            "Starting full import of users from Microsoft Graph (pageSize={})",
            configUser.userpagingsize
        )

        val firstPage = callGraph {
            graphServiceClient.users()
                .delta()
                .get { req ->
                    req.queryParameters?.select = selection
                    req.queryParameters?.top = configUser.userpagingsize
                }
        }

        val result = pageThroughUsers(firstPage, isFullImport = true)
        markNotSeenUsers(runStartTime)
        val cutoff = Instant.now().minus(configUser.staleAfterDays.toLong(), ChronoUnit.DAYS)
        val deleted = withContext(Dispatchers.IO) {
            entraUserSyncService.finishFullImport(cutoff)
        }

        log.info(
            "Full import completed (fetchedTotal={}, publishedChanged={}, publishedDeleted={})",
            result.totalUsersSeen, result.publishedUsers, deleted
        )
    }

    private suspend fun markNotSeenUsers(startTime: Instant) {
        val staleIds = withContext(Dispatchers.IO) {
            coreUserRepository.findStaleObjectIds(startTime)
        }
        log.info("Marking {} stale users as not seen", staleIds.size)

        coreUserRepository.incrementNotSeenCount(staleIds)
    }

    private fun shouldContinueWithImport(): Boolean {
        val totalCountSource = graphServiceClient
            .users()
            .count()
            .get { requestConfiguration ->
                requestConfiguration.headers.add("ConsistencyLevel", "eventual")
                requestConfiguration.queryParameters?.filter = "userType eq 'Member'"
            } ?: 0
        val totalCountDb = coreUserRepository.getCount()
        if (totalCountDb != 0 && Math.abs(totalCountSource - totalCountDb).div(totalCountDb) < Math.divideExact(
                configUser.acceptedDeviationPercent ?: 0,
                100
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
        isFullImport: Boolean
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
                    "Users page {} fetched={} (fetchedTotalSoFar={})", pageNo, fetchedThisPage, totalUsersFetched
                )

                val publishedThisPage = withContext(Dispatchers.IO) {
                    entraUserSyncService.processPage(value)
                }
                totalPublished += publishedThisPage
            } else {
                log.debug("Users page {} fetched=0", pageNo)
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
                    current = callGraph {
                        graphServiceClient.users()
                            .delta()
                            .withUrl(next)
                            .get { req -> req.headers.add("Prefer", "return=minimal") }
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
                    initialRun, totalUsersFetched, totalPublished
                )
            } else {
                log.info("Stored new deltaLink after full import")
            }
        }

        return PageResult(totalUsersFetched, totalPublished)
    }

    private suspend fun <T> callGraph(block: () -> T): T {
        return try {
            withContext(Dispatchers.IO) { block() }
        } catch (ae: ApiException) {
            log.error("Graph call failed: {}", ae.message)
            throw ae
        } catch (e: Exception) {
            throw if (e is RuntimeException) e else CompletionException(e)
        }
    }

    private fun logElapsed(startTimeMs: Long, operation: String) {
        val elapsed = System.currentTimeMillis() - startTimeMs
        val minutes = (elapsed / 1000) / 60
        val seconds = (elapsed / 1000) % 60
        log.info("Finished {} in {}m {}s", operation, minutes, seconds)
    }

    private data class PageResult(val totalUsersSeen: Int, val publishedUsers: Int)

    companion object {
        private val log = LoggerFactory.getLogger(MsGraphUser::class.java)
    }
}
