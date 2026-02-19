package no.novari.msgraphgateway.group

import com.microsoft.graph.serviceclient.GraphServiceClient
import no.novari.msgraphgateway.config.Config
import no.novari.msgraphgateway.config.ConfigGroup
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

class MsGraphGroup {
    protected val config: Config? = null
    protected val configGroup: ConfigGroup? = null
    protected val graphServiceClient: GraphServiceClient? = null

//    private val resourceGroupMembershipCache: ConcurrentHashMap<String?, Optional<ResourceGroupMembership?>?>? = null
    private val membershipCache: MutableSet<String?> = ConcurrentHashMap.newKeySet<String?>()
    private val azureGroupCache: ConcurrentHashMap<String?, MsGraphGroup?>? = null

//    private val azureGroupProducerService: AzureGroupProducerService? = null
//    private val azureGroupMembershipProducerService: AzureGroupMembershipProducerService? = null
    private val groupExecutor: ExecutorService = Executors.newFixedThreadPool(10)
    private var fullImport = false
    private var odataGroupDeltaLink: String? = null
    private var numMembers = AtomicInteger(0)
    private val addedMemberships = AtomicInteger(0)
    private val removedMemberships = AtomicInteger(0)
    private var groupCounter: AtomicInteger? = null
    private var processedGroupIds: MutableSet<String?>? = null
}
