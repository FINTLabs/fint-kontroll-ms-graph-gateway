package no.novari.msgraphgateway.group

import com.microsoft.graph.groups.delta.DeltaGetResponse
import com.microsoft.graph.models.Group
import com.microsoft.graph.serviceclient.GraphServiceClient
import com.microsoft.kiota.ApiException
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import no.novari.msgraphgateway.config.ConfigGroup
import no.novari.msgraphgateway.config.ConfigUser
import no.novari.msgraphgateway.dto.UserWithGroupsDto
import no.novari.msgraphgateway.entra.DeltaLinkStore
import no.novari.msgraphgateway.entra.group.EntraGroup
import no.novari.msgraphgateway.entra.user.EntraUser
import no.novari.msgraphgateway.services.group.EntraGroupMembershipSyncService
import no.novari.msgraphgateway.services.group.EntraGroupSyncService
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.web.server.ResponseStatusException
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletionException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

@Component
class MsGraphGroup(
    private val configGroup: ConfigGroup,
    private val graphServiceClient: GraphServiceClient,
    private val groupSyncService: EntraGroupSyncService,
    private val groupMembershipSyncService: EntraGroupMembershipSyncService,
    private val deltaLinkStore: DeltaLinkStore,
    private val configUser: ConfigUser,
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val runMutex = Mutex()
    private val fullImportRequested = AtomicBoolean(false)
    private val republishAllRequested = AtomicBoolean(false)

    @Volatile
    private var groupDeltaLink: String? = null

    @Volatile
    private var membershipBootstrapCompleted: Boolean = false

    @PostConstruct
    fun loadDeltaLink() {
        membershipBootstrapCompleted = deltaLinkStore.find(GROUP_MEMBERSHIP_BOOTSTRAP_STATE) == BOOTSTRAP_COMPLETED
        groupDeltaLink =
            deltaLinkStore
                .find(GROUP_MEMBERSHIP_DELTA_STATE)
                ?.takeIf { membershipBootstrapCompleted }

        if (!groupDeltaLink.isNullOrBlank()) {
            log.info("Loaded persisted groups deltaLink from DB")
        } else if (!membershipBootstrapCompleted) {
            log.info("Membership bootstrap is not completed; a full group and membership snapshot is required")
        } else {
            log.info("No persisted groups deltaLink found (first run)")
        }
    }

    @PreDestroy
    fun shutdown() {
        scope.cancel("MsGraphGroup shutting down")
    }

    @Scheduled(
        initialDelayString = $$"${novari.scheduler.group.delta.initial-delay-ms}",
        fixedDelayString = $$"${novari.scheduler.group.delta.fixed-delay-ms}",
    )
    fun pullAllGroupsDelta() {
        if (fullImportRequested.get()) {
            log.info("Full group import pending; trying to start it instead of delta run")
            scope.launch {
                tryStartFullImportIfRequested()
            }
            return
        }

        scope.launch {
            if (!runMutex.tryLock()) {
                log.info("Group sync already running; skipping delta run")
                return@launch
            }

            val startTime = System.currentTimeMillis()
            val trackingId = UUID.randomUUID().toString()

            try {
                if (groupDeltaLink.isNullOrBlank()) {
                    log.info("No group deltaLink present; starting group and membership bootstrap")
                    startFullImport(false)
                    return@launch
                }

                val selection = configGroup.getGroupAttributesNotMembers()
                val deltaPresent = !groupDeltaLink.isNullOrBlank()

                log.info("Starting groups delta pull from Microsoft Graph")
                log.debug(
                    "TrackingId {} (deltaLinkPresent={}, pageSize={})",
                    trackingId,
                    deltaPresent,
                    configGroup.groupPagingSize,
                )

                fun buildInitialRequest(link: String?): DeltaGetResponse? =
                    if (!link.isNullOrBlank()) {
                        graphServiceClient
                            .groups()
                            .delta()
                            .withUrl(link)
                            .get { req ->
                                req.headers.add("client-request-id", trackingId)
                            }
                    } else {
                        graphServiceClient
                            .groups()
                            .delta()
                            .get { req ->
                                req.headers.add("client-request-id", trackingId)
                                req.headers.add("ConsistencyLevel", "eventual")
                                req.queryParameters?.apply {
                                    select = selection
                                    top = configGroup.groupPagingSize
                                }
                            }
                    }

                val firstPage =
                    try {
                        callGraph { buildInitialRequest(groupDeltaLink) }
                    } catch (exception: ApiException) {
                        if (!groupDeltaLink.isNullOrBlank() && exception.isInvalidDeltaState()) {
                            log.warn("Group deltaLink is invalid; starting a fresh group and membership bootstrap")

                            groupDeltaLink = null
                            withContext(Dispatchers.IO) {
                                deltaLinkStore.createOrUpdate(GROUP_MEMBERSHIP_DELTA_STATE, "")
                            }

                            startFullImport(false)
                            return@launch
                        } else {
                            throw exception
                        }
                    }

                pageThroughGroups(
                    firstPage = firstPage,
                    isFullImport = false,
                    notSeenIncremented = mutableSetOf(),
                    republishAll = false,
                )
            } catch (e: RuntimeException) {
                log.error("Delta groups pull failed: {}", e.message, e)
            } finally {
                runMutex.unlock()
                logElapsed(startTime, "delta groups pull")
                tryStartFullImportIfRequested()
            }
        }
    }

    @Scheduled(cron = $$"${novari.scheduler.group.full-import.cron}")
    fun fullImportGroups() {
        requestFullImport(false)
    }

    @Scheduled(cron = $$"${novari.scheduler.group.weekly-publish.cron}")
    fun weeklyPublishGroups() {
        requestFullImport(true)
    }

    fun requestFullImport(republishAll: Boolean = false) {
        fullImportRequested.set(true)

        if (republishAll) {
            republishAllRequested.set(true)
        }

        scope.launch {
            if (!runMutex.tryLock()) {
                log.info("A sync is running; full group import requested and will start afterward")
                return@launch
            }

            val startTime = System.currentTimeMillis()
            val republishRequested = republishAllRequested.getAndSet(false)

            try {
                startFullImport(republishRequested)
            } finally {
                runMutex.unlock()
                logElapsed(startTime, "full import of groups")
                fullImportRequested.set(false)
            }
        }
    }

    private suspend fun tryStartFullImportIfRequested() {
        if (!fullImportRequested.get()) return
        if (!runMutex.tryLock()) {
            log.info("Full group import pending, but group sync is still running")
            return
        }

        val startTime = System.currentTimeMillis()
        val republishRequested = republishAllRequested.getAndSet(false)

        try {
            startFullImport(republishRequested)
        } finally {
            runMutex.unlock()
            logElapsed(startTime, "full import of groups")
            fullImportRequested.set(false)
        }
    }

    suspend fun startFullImport(republishAll: Boolean = false) {
        val runStartTime = Instant.now()
        val snapshotRunId = UUID.randomUUID()
        val initialBootstrap = !membershipBootstrapCompleted
        val selection = configGroup.getGroupAttributesNotMembers()
        val notSeenIncremented = mutableSetOf<UUID>()

        log.info("Starting full import of groups and memberships from Microsoft Graph. snapshotRunId={}", snapshotRunId)
        try {
            val bootstrapDeltaLink = acquireLatestMembershipDeltaLink(selection)
            val groups = fetchAllGroups(notSeenIncremented, republishAll, selection)
            val totalMemberships = bootstrapMemberships(snapshotRunId, groups)
            val firstDeltaPage =
                callGraph {
                    graphServiceClient
                        .groups()
                        .delta()
                        .withUrl(bootstrapDeltaLink)
                        .get()
                }
            val result =
                pageThroughGroups(
                    firstPage = firstDeltaPage,
                    isFullImport = true,
                    notSeenIncremented = notSeenIncremented,
                    republishAll = republishAll,
                    persistDeltaLink = false,
                    membershipSnapshotRunId = snapshotRunId,
                )

            val finalDeltaLink =
                result.deltaLink
                    ?.takeIf { it.isNotBlank() }
                    ?: error("Microsoft Graph did not return a deltaLink after group membership catch-up")

            val membershipResult =
                withContext(Dispatchers.IO) {
                    groupMembershipSyncService.completeSnapshot(
                        runId = snapshotRunId,
                        initialBootstrap = initialBootstrap,
                        republishAll = republishAll,
                    )
                }

            groupSyncService.markNotSeenGroups(runStartTime, notSeenIncremented)
            val deletedGroups = groupSyncService.finishFullImport(runStartTime)

            storeGroupDeltaLink(finalDeltaLink)
            if (initialBootstrap) {
                withContext(Dispatchers.IO) {
                    deltaLinkStore.createOrUpdate(GROUP_MEMBERSHIP_BOOTSTRAP_STATE, BOOTSTRAP_COMPLETED)
                }
                membershipBootstrapCompleted = true
            }

            log.info(
                "Full import of groups and memberships completed " +
                    "(Total groups={}, total memberships={}, deltaChanges={}, membershipChanges={}, " +
                    "membershipAdded={}, membershipRemoved={}, publishedChanged={}, deleted={})",
                groups.size,
                totalMemberships,
                result.totalGroupsSeen,
                result.membershipChanges,
                membershipResult.added,
                membershipResult.removed,
                result.publishedGroups,
                deletedGroups,
            )
        } finally {
            withContext(Dispatchers.IO) {
                groupMembershipSyncService.discardSnapshot(snapshotRunId)
            }
        }
    }

    private suspend fun acquireLatestMembershipDeltaLink(selection: Array<String>): String {
        val trackedSelection = (selection.asList() + MEMBERS_ATTRIBUTE).distinct().joinToString(",")
        val response =
            callGraph {
                graphServiceClient
                    .groups()
                    .delta()
                    .withUrl("$GROUPS_DELTA_URL?\$deltatoken=latest&\$select=$trackedSelection")
                    .get()
            }

        return response
            ?.odataDeltaLink
            ?.takeIf { it.isNotBlank() }
            ?: error("Microsoft Graph did not return a deltaLink for group membership bootstrap")
    }

    private suspend fun fetchAllGroups(
        notSeenIncremented: MutableSet<UUID>,
        republishAll: Boolean,
        selection: Array<String>,
    ): List<Group> {
        val matchingGroups = mutableListOf<Group>()
        var response =
            callGraph {
                graphServiceClient.groups().get { req ->
                    req.headers.add("ConsistencyLevel", "eventual")
                    req.queryParameters?.apply {
                        select = selection
                        top = configGroup.groupPagingSize
                    }
                }
            }

        while (response != null) {
            val groups = response.value.orEmpty()
            groupSyncService.processPage(groups, notSeenIncremented, republishAll)
            groups.filterTo(matchingGroups, groupSyncService::matchesConfiguredGroup)

            response =
                response.odataNextLink
                    ?.takeIf { it.isNotBlank() }
                    ?.let { nextLink -> callGraph { graphServiceClient.groups().withUrl(nextLink).get() } }
        }

        return matchingGroups.distinctBy { it.id }
    }

    private suspend fun bootstrapMemberships(
        snapshotRunId: UUID,
        groups: List<Group>,
    ): Int {
        if (groups.isEmpty()) return 0

        val nextGroupIndex = AtomicInteger(0)
        val totalMemberships = AtomicInteger(0)
        val workerCount = minOf(MEMBERSHIP_WORKERS, groups.size)

        coroutineScope {
            List(workerCount) {
                async(Dispatchers.IO) {
                    while (true) {
                        val index = nextGroupIndex.getAndIncrement()
                        if (index >= groups.size) return@async

                        val groupId = groups[index].id?.let { runCatching { UUID.fromString(it) }.getOrNull() }
                        if (groupId == null) {
                            error("Cannot complete membership snapshot: invalid group object ID ${groups[index].id}")
                        }

                        val members = fetchAllGroupMembers(groupId.toString())
                        log.debug("Fetched {} direct members for groupId {}", members.size, groupId)
                        groupMembershipSyncService.stageGroupMemberships(snapshotRunId, groupId, members)
                        totalMemberships.addAndGet(members.size)
                    }
                }
            }.awaitAll()
        }

        return totalMemberships.get()
    }

    private fun fetchAllGroupMembers(groupId: String): List<com.microsoft.graph.models.DirectoryObject> {
        val members = mutableListOf<com.microsoft.graph.models.DirectoryObject>()
        var response =
            graphServiceClient
                .groups()
                .byGroupId(groupId)
                .members()
                .get { req ->
                    req.queryParameters?.apply {
                        select = arrayOf("id")
                        top = GRAPH_MAX_PAGE_SIZE
                    }
                }

        while (response != null) {
            members += response.value.orEmpty()
            response =
                response.odataNextLink
                    ?.takeIf { it.isNotBlank() }
                    ?.let { nextLink ->
                        graphServiceClient
                            .groups()
                            .byGroupId(groupId)
                            .members()
                            .withUrl(nextLink)
                            .get()
                    }
        }

        return members
    }

    private suspend fun pageThroughGroups(
        firstPage: DeltaGetResponse?,
        isFullImport: Boolean,
        republishAll: Boolean,
        notSeenIncremented: MutableSet<UUID>,
        persistDeltaLink: Boolean = true,
        membershipSnapshotRunId: UUID? = null,
    ): PageResult {
        var current: DeltaGetResponse? = firstPage
        var last: DeltaGetResponse? = firstPage

        var totalGroupsFetched = 0
        var totalPublished = 0
        var totalMembershipChanges = 0
        var pageNo = 0

        val seenNextLinks = HashSet<String>()

        while (current != null) {
            pageNo++

            val value = current.value
            val fetchedThisPage = value?.size ?: 0

            if (fetchedThisPage > 0) {
                totalGroupsFetched += fetchedThisPage

                log.debug(
                    "Groups page {} fetched={} (fetchedTotalSoFar={})",
                    pageNo,
                    fetchedThisPage,
                    totalGroupsFetched,
                )

                val publishedThisPage =
                    withContext(Dispatchers.IO) {
                        groupSyncService.processPage(value, notSeenIncremented, republishAll)
                    }

                totalMembershipChanges +=
                    withContext(Dispatchers.IO) {
                        groupMembershipSyncService.processDeltaPage(
                            value.orEmpty().filter { group ->
                                group.additionalData.containsKey("@removed") ||
                                    groupSyncService.matchesConfiguredGroup(group)
                            },
                            membershipSnapshotRunId,
                        )
                    }

                totalPublished += publishedThisPage
            } else {
                log.debug("Groups page {} fetched=0", pageNo)
                log.trace(current.toString())
            }

            last = current

            val next = current.odataNextLink

            current =
                if (next.isNullOrBlank()) {
                    null
                } else {
                    if (!seenNextLinks.add(next)) {
                        log.error("Detected group nextLink cycle; stopping paging (nextLink={})", next)
                        null
                    } else {
                        callGraph {
                            graphServiceClient
                                .groups()
                                .delta()
                                .withUrl(next)
                                .get()
                        }
                    }
                }
        }

        val newDelta = last?.odataDeltaLink

        if (newDelta.isNullOrBlank()) {
            log.error("Last group page does not contain @odata.deltaLink; deltaLink not updated")
        } else {
            val initialRun = groupDeltaLink.isNullOrEmpty()
            if (persistDeltaLink) storeGroupDeltaLink(newDelta)

            if (!isFullImport) {
                log.info(
                    "Delta groups pull complete " +
                        "(initialRun={}, fetchedTotal={}, membershipChanges={}, publishedChanged={})",
                    initialRun,
                    totalGroupsFetched,
                    totalMembershipChanges,
                    totalPublished,
                )
            } else {
                log.info("Group membership delta catch-up completed")
            }
        }

        return PageResult(totalGroupsFetched, totalPublished, totalMembershipChanges, newDelta)
    }

    private suspend fun storeGroupDeltaLink(deltaLink: String) {
        groupDeltaLink = deltaLink
        withContext(Dispatchers.IO) {
            deltaLinkStore.createOrUpdate(GROUP_MEMBERSHIP_DELTA_STATE, deltaLink)
        }
    }

    fun getEntraUserWithGroups(userId: String): UserWithGroupsDto =
        try {
            val selection = configUser.userAttributesDelta()
            val user =
                graphServiceClient.users().byUserId(userId).get { req ->
                    req.queryParameters?.select = selection
                }
                    ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "User not found: $userId")

            val entraUser = EntraUser(user, configUser)
            val entraGroups = getTransitiveFintKontrollGroups(userId)

            log.info(
                "*** <<< Rest service found userId {}. User is member of {} FINT kontroll groups >>> ***",
                userId,
                entraGroups.size,
            )

            UserWithGroupsDto(entraUser, entraGroups)
        } catch (ex: Exception) {
            log.error("Failed to fetch user or groups: {}", ex.message)
            UserWithGroupsDto()
        }

    private fun getTransitiveFintKontrollGroups(userId: String): List<EntraGroup> {
        val entraGroups = mutableListOf<EntraGroup>()
        val selectionCriteriaGroup = groupLookupSelection()

        var response =
            graphServiceClient
                .users()
                .byUserId(userId)
                .transitiveMemberOf()
                .graphGroup()
                .get { requestConfig ->
                    requestConfig.queryParameters?.select = selectionCriteriaGroup
                    requestConfig.queryParameters?.top = 999
                }

        while (response != null) {
            response.value
                .orEmpty()
                .asSequence()
                .filter(::isWantedGroup)
                .map { EntraGroup(it, configGroup) }
                .toCollection(entraGroups)

            response =
                response.odataNextLink
                    ?.takeIf { it.isNotBlank() }
                    ?.let { nextLink ->
                        graphServiceClient
                            .users()
                            .byUserId(userId)
                            .transitiveMemberOf()
                            .graphGroup()
                            .withUrl(nextLink)
                            .get()
                    }
        }

        return entraGroups
    }

    private fun groupLookupSelection(): Array<String> =
        arrayOf(
            "id",
            "displayName",
            "securityEnabled",
            configGroup.resourceGroupIdAttribute ?: "",
        ).filter { it.isNotBlank() }.toTypedArray()

    private fun isWantedGroup(group: Group): Boolean {
        if (group.securityEnabled != true) {
            return false
        }

        val name = group.displayName ?: return false
        val prefix = configGroup.prefix?.trim().orEmpty()
        val suffix = configGroup.suffix?.trim().orEmpty()

        val matchesName =
            when (configGroup.filterMode) {
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

        val hasResourceAttr =
            configGroup.resourceGroupIdAttribute
                ?.takeIf { it.isNotBlank() }
                ?.let { key -> group.additionalData.containsKey(key) }
                ?: true

        return matchesName && hasResourceAttr
    }

    fun getGroupInfo(groupId: String): EntraGroup? =
        try {
            val group =
                graphServiceClient
                    .groups()
                    .byGroupId(groupId)
                    .get()
                    ?: return null

            EntraGroup(group, configGroup)
        } catch (ex: ApiException) {
            if (ex.responseStatusCode == 404) {
                log.info("Group with objectId {} not found in Entra", groupId)
                null
            } else {
                throw ex
            }
        }

    fun getGroupInfoByResourceGroupId(resourceGroupId: Long): EntraGroup? {
        val attr = configGroup.resourceGroupIdAttribute ?: return null

        val groups =
            graphServiceClient
                .groups()
                .get { req ->
                    req.queryParameters?.select =
                        arrayOf(
                            "id",
                            "displayName",
                            attr,
                        )
                }?.value
                ?: return null

        val group =
            groups.firstOrNull {
                it.additionalData[attr]?.toString()?.toLongOrNull() == resourceGroupId
            } ?: return null

        return EntraGroup(group, configGroup)
    }

    private fun ApiException.isInvalidDeltaState(): Boolean =
        responseStatusCode == 400 || responseStatusCode == 404 || responseStatusCode == 410

    private suspend fun <T> callGraph(block: () -> T): T =
        try {
            withContext(Dispatchers.IO) { block() }
        } catch (apiException: ApiException) {
            log.error(
                "Graph group call failed with error code {}. {}",
                apiException.responseStatusCode,
                apiException.message,
            )
            throw apiException
        } catch (exception: Exception) {
            throw exception as? RuntimeException ?: CompletionException(exception)
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
        val totalGroupsSeen: Int,
        val publishedGroups: Int,
        val membershipChanges: Int,
        val deltaLink: String?,
    )

    companion object {
        private const val GROUPS_DELTA_URL = "https://graph.microsoft.com/v1.0/groups/delta"
        private const val GROUP_MEMBERSHIP_DELTA_STATE = "groups-with-members"
        private const val GROUP_MEMBERSHIP_BOOTSTRAP_STATE = "groups-with-members-bootstrap"
        private const val BOOTSTRAP_COMPLETED = "true"
        private const val MEMBERS_ATTRIBUTE = "members"
        private const val MEMBERSHIP_WORKERS = 8
        private const val GRAPH_MAX_PAGE_SIZE = 999
        private val log = LoggerFactory.getLogger(MsGraphGroup::class.java)
    }
}
