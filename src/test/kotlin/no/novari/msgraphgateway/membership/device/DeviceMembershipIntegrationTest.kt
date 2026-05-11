package no.novari.msgraphgateway.membership.device

import com.fasterxml.jackson.databind.ObjectMapper
import com.microsoft.graph.core.content.BatchRequestContent
import com.microsoft.graph.core.content.BatchResponseContent
import com.microsoft.graph.core.requests.BatchRequestBuilder
import com.microsoft.graph.serviceclient.GraphServiceClient
import com.microsoft.kiota.RequestAdapter
import com.microsoft.kiota.RequestInformation
import com.microsoft.kiota.serialization.Parsable
import com.microsoft.kiota.serialization.ParsableFactory
import com.microsoft.kiota.serialization.SerializationWriter
import com.microsoft.kiota.serialization.SerializationWriterFactory
import io.mockk.every
import io.mockk.mockk
import no.novari.msgraphgateway.Application
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import okhttp3.ResponseBody.Companion.toResponseBody
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.support.serializer.JsonSerializer
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.io.ByteArrayInputStream
import java.time.Duration
import java.util.Properties
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

@Testcontainers
@SpringBootTest(
    classes = [
        Application::class,
        DeviceMembershipIntegrationTest.KafkaTopicConfiguration::class,
        DeviceMembershipIntegrationTest.GraphClientConfiguration::class,
    ],
    properties = [
        "fint.application-id=fint-kontroll-ms-graph-gateway",
        "fint.kontroll.opa.url=http://localhost/opa",
        "ms-graph.credentials.clientid=00000000-0000-0000-0000-000000000001",
        "ms-graph.credentials.clientsecret=test-secret",
        "ms-graph.credentials.tenantguid=00000000-0000-0000-0000-000000000002",
        "novari.scheduler.user.delta.initial-delay-ms=86400000",
        "novari.scheduler.user.delta.fixed-delay-ms=86400000",
        "novari.scheduler.user.full-import.cron=-",
        "ms-graph.membership.device.consumer-concurrency=1",
        "ms-graph.membership.device.consumer-max-poll-records=500",
        "ms-graph.membership.device.graph-batch-size=20",
        "ms-graph.membership.device.graph-max-concurrent-calls=3",
        "ms-graph.membership.device.result-topic-partitions=1",
        "spring.task.scheduling.enabled=false",
        "logging.level.no.novari=ERROR",
    ],
)
@Tag("manual")
class DeviceMembershipIntegrationTest {
    @Autowired
    private lateinit var jdbcTemplate: JdbcTemplate

    @Autowired
    private lateinit var kontrollMembershipConsumer:
        ConcurrentMessageListenerContainer<String, DeviceResourceGroupMembership>

    @Autowired
    private lateinit var graphCallCounter: AtomicInteger

    @Test
    fun membershipConsumerHandlesALotOfKafkaMessagesInReasonableTime() {
        val graphResponseDelay = graphResponseDelay()
        val runId = UUID.randomUUID().toString()

        graphCallCounter.set(0)
        waitForConsumerAssignment()

        val startedAt = System.nanoTime()
        produceMembershipMessages(runId, PERFORMANCE_TEST_MESSAGE_COUNT, OperationType.ADD)
        val consumedResults = consumeResultMessages(runId, PERFORMANCE_TEST_TIMEOUT, PERFORMANCE_TEST_MESSAGE_COUNT)
        val elapsed = Duration.ofNanos(System.nanoTime() - startedAt)

        assertEquals(
            PERFORMANCE_TEST_MESSAGE_COUNT,
            consumedResults,
            "Expected every consumed membership event to produce one entra-device-group-membership result within $PERFORMANCE_TEST_TIMEOUT",
        )
        assertTrue(
            graphCallCounter.get() > 0,
            "Expected fresh membership events to call the fake Graph batch endpoint",
        )

        println(
            "Membership consumer processed $consumedResults messages with " +
                "${graphCallCounter.get()} Graph batch calls at ${graphResponseDelay.toMillis()} ms/call " +
                "in ${elapsed.toMillis()} ms " +
                "(${consumedResults * 1000 / elapsed.toMillis().coerceAtLeast(1)} msg/s)",
        )
    }

    @Test
    fun membershipConsumerHandlesFreshAddLoadAsAdded() {
        val memberships = createMemberships(CORRECTNESS_TEST_MESSAGE_COUNT, OperationType.ADD)

        checkEntraStatuses(
            memberships = memberships,
            expectedStatuses = mapOf(EntraStatus.ADDED to CORRECTNESS_TEST_MESSAGE_COUNT),
            expectGraphCalls = true,
        )
    }

    @Test
    fun membershipConsumerHandlesExistingAddedAddLoadAsNoChanges() {
        val memberships = createMemberships(CORRECTNESS_TEST_MESSAGE_COUNT, OperationType.ADD)
        saveExistingMemberships(memberships, EntraStatus.ADDED)

        checkEntraStatuses(
            memberships = memberships,
            expectedStatuses = mapOf(EntraStatus.NO_CHANGES to CORRECTNESS_TEST_MESSAGE_COUNT),
            expectGraphCalls = false,
        )
    }

    @Test
    fun membershipConsumerHandlesExistingRemovedRemoveLoadAsNoChanges() {
        val memberships = createMemberships(CORRECTNESS_TEST_MESSAGE_COUNT, OperationType.REMOVE)
        saveExistingMemberships(memberships, EntraStatus.REMOVED)

        checkEntraStatuses(
            memberships = memberships,
            expectedStatuses = mapOf(EntraStatus.NO_CHANGES to CORRECTNESS_TEST_MESSAGE_COUNT),
            expectGraphCalls = false,
        )
    }

    @Test
    fun membershipConsumerHandlesExistingRemovedAddLoadAsAdded() {
        val memberships = createMemberships(CORRECTNESS_TEST_MESSAGE_COUNT, OperationType.ADD)
        saveExistingMemberships(memberships, EntraStatus.REMOVED)

        checkEntraStatuses(
            memberships = memberships,
            expectedStatuses = mapOf(EntraStatus.ADDED to CORRECTNESS_TEST_MESSAGE_COUNT),
            expectGraphCalls = true,
        )
    }

    @Test
    fun membershipConsumerHandlesMixedAddRemoveAndGraph400Responses() {
        val scenario = createMixedAddRemove400Scenario()

        checkEntraStatuses(
            memberships = scenario.memberships,
            graphErrorGroupRefs = scenario.graphErrorGroupRefs,
            expectedStatuses = scenario.expectedStatuses,
            expectGraphCalls = true,
        )
    }

    private fun createMemberships(
        count: Int,
        operation: OperationType,
    ): List<DeviceResourceGroupMembership> =
        List(count) {
            DeviceResourceGroupMembership(
                operation = operation,
                entraGroupRef = UUID.randomUUID().toString(),
                entraDeviceRef = UUID.randomUUID().toString(),
            )
        }

    private fun checkEntraStatuses(
        memberships: List<DeviceResourceGroupMembership>,
        graphErrorGroupRefs: Set<String> = emptySet(),
        expectedStatuses: Map<EntraStatus, Int>,
        expectGraphCalls: Boolean,
    ) {
        val runId = UUID.randomUUID().toString()

        graphCallCounter.set(0)
        GraphClientConfiguration.reset()
        GraphClientConfiguration.returnBadRequestForGroups(graphErrorGroupRefs)
        waitForConsumerAssignment()

        val startedAt = System.nanoTime()
        produceMembershipMessages(runId, memberships)
        val consumedResults = consumeResultMessagesByStatus(runId, memberships.size, CORRECTNESS_TEST_TIMEOUT)
        val elapsed = Duration.ofNanos(System.nanoTime() - startedAt)

        assertEquals(
            expectedStatuses,
            consumedResults,
            "Expected membership statuses within $CORRECTNESS_TEST_TIMEOUT",
        )
        if (expectGraphCalls) {
            assertTrue(
                graphCallCounter.get() > 0,
                "Expected membership events to call the fake Graph batch endpoint",
            )
        } else {
            assertEquals(0, graphCallCounter.get(), "Expected membership events to skip Graph")
        }

        println(
            "Membership consumer processed ${memberships.size} status-checked messages with " +
                "${graphCallCounter.get()} Graph batch calls in ${elapsed.toMillis()} ms",
        )
    }

    private fun saveExistingMemberships(memberships: List<Pair<DeviceResourceGroupMembership, EntraStatus>>) {
        memberships.chunked(DB_BATCH_SIZE).forEach { chunk ->
            jdbcTemplate.batchUpdate(
                """
                INSERT INTO device_memberships (device_ref, group_ref, status, created_at, last_updated_at)
                VALUES (?::uuid, ?::uuid, ?, now(), now())
                """.trimIndent(),
                chunk,
                chunk.size,
            ) { ps, (membership, status) ->
                ps.setString(1, membership.entraDeviceRef)
                ps.setString(2, membership.entraGroupRef)
                ps.setString(3, status.name)
            }
        }
    }

    private fun saveExistingMemberships(
        memberships: List<DeviceResourceGroupMembership>,
        status: EntraStatus,
    ) {
        saveExistingMemberships(memberships.map { it to status })
    }

    private fun waitForConsumerAssignment() {
        val deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos()

        while (kontrollMembershipConsumer.assignedPartitions.isEmpty()) {
            check(System.nanoTime() < deadline) {
                "Timed out waiting for membership consumer partition assignment"
            }
            Thread.sleep(100)
        }
    }

    private fun produceMembershipMessages(
        runId: String,
        memberships: List<DeviceResourceGroupMembership>,
    ) {
        KafkaProducer<String, DeviceResourceGroupMembership>(producerProperties()).use { producer ->
            memberships.forEachIndexed { index, membership ->
                producer.send(
                    ProducerRecord(
                        INPUT_TOPIC,
                        "$runId-membership-$index",
                        membership,
                    ),
                )
            }
            producer.flush()
        }
    }

    private fun produceMembershipMessages(
        runId: String,
        count: Int,
        operation: OperationType,
    ) {
        KafkaProducer<String, DeviceResourceGroupMembership>(producerProperties()).use { producer ->
            repeat(count) { index ->
                producer.send(
                    ProducerRecord(
                        INPUT_TOPIC,
                        "$runId-membership-$index",
                        DeviceResourceGroupMembership(
                            operation = operation,
                            entraGroupRef = UUID.randomUUID().toString(),
                            entraDeviceRef = UUID.randomUUID().toString(),
                        ),
                    ),
                )
            }
            producer.flush()
        }
    }

    private fun consumeResultMessages(
        runId: String,
        timeout: Duration,
        expectedCount: Int,
    ): Int = consumeResultKeys(runId, expectedCount, timeout).size

    private fun consumeResultMessagesByStatus(
        runId: String,
        expectedCount: Int,
        timeout: Duration,
    ): Map<EntraStatus, Int> {
        KafkaConsumer<String, ByteArray>(consumerProperties()).use { consumer ->
            consumer.subscribe(listOf(RESULT_TOPIC))

            val deadline = System.nanoTime() + timeout.toNanos()
            val consumedStatuses = mutableMapOf<EntraStatus, Int>()
            val consumedKeys = HashSet<String>(expectedCount)
            val keyPrefix = "$runId-membership-"

            while (consumedKeys.size < expectedCount && System.nanoTime() < deadline) {
                consumer
                    .poll(Duration.ofMillis(500))
                    .forEach { record ->
                        val key = record.key()
                        if (key != null && key.startsWith(keyPrefix) && consumedKeys.add(key)) {
                            val status = objectMapper.readTree(record.value()).get("code").asText()
                            val code = EntraStatus.valueOf(status)
                            consumedStatuses[code] = consumedStatuses.getOrDefault(code, 0) + 1
                        }
                    }
            }

            return consumedStatuses
        }
    }

    private fun consumeResultKeys(
        runId: String,
        expectedCount: Int,
        timeout: Duration,
    ): Set<String> {
        KafkaConsumer<String, ByteArray>(consumerProperties()).use { consumer ->
            consumer.subscribe(listOf(RESULT_TOPIC))

            val deadline = System.nanoTime() + timeout.toNanos()
            val consumedKeys = HashSet<String>(expectedCount)
            val keyPrefix = "$runId-membership-"

            while (consumedKeys.size < expectedCount && System.nanoTime() < deadline) {
                consumer
                    .poll(Duration.ofMillis(500))
                    .forEach { record ->
                        val key = record.key()
                        if (key != null && key.startsWith(keyPrefix)) {
                            consumedKeys += key
                        }
                    }
            }

            return consumedKeys
        }
    }

    private fun createMixedAddRemove400Scenario(): MixedAddRemove400Scenario {
        val memberships = mutableListOf<DeviceResourceGroupMembership>()
        val existingMemberships = mutableListOf<Pair<DeviceResourceGroupMembership, EntraStatus>>()
        val graphErrorGroupRefs = mutableSetOf<String>()
        val expectedStatuses =
            mutableMapOf(
                EntraStatus.ADDED to 0,
                EntraStatus.REMOVED to 0,
                EntraStatus.ERROR to 0,
                EntraStatus.NO_CHANGES to 0,
                EntraStatus.FAILED to 0,
            )

        repeat(8) { index ->
            val operation = if (index % 2 == 0) OperationType.ADD else OperationType.REMOVE
            val membership =
                DeviceResourceGroupMembership(
                    operation = operation,
                    entraGroupRef = UUID.randomUUID().toString(),
                    entraDeviceRef = UUID.randomUUID().toString(),
                )

            when (index) {
                0 -> {
                    existingMemberships += membership to EntraStatus.ADDED
                    expectedStatuses.increment(EntraStatus.NO_CHANGES)
                }

                1 -> {
                    existingMemberships += membership to EntraStatus.ADDED
                    expectedStatuses.increment(EntraStatus.REMOVED)
                }

                2 -> {
                    expectedStatuses.increment(EntraStatus.ADDED)
                }

                3 -> {
                    graphErrorGroupRefs += membership.entraGroupRef
                    expectedStatuses.increment(EntraStatus.FAILED)
                }

                4 -> {
                    graphErrorGroupRefs += membership.entraGroupRef
                    expectedStatuses.increment(EntraStatus.ERROR)
                }

                5 -> {
                    existingMemberships += membership to EntraStatus.REMOVED
                    expectedStatuses.increment(EntraStatus.NO_CHANGES)
                }

                6 -> {
                    expectedStatuses.increment(EntraStatus.ADDED)
                }

                7 -> {
                    graphErrorGroupRefs += membership.entraGroupRef
                    expectedStatuses.increment(EntraStatus.FAILED)
                }
            }

            memberships += membership
        }

        saveExistingMemberships(existingMemberships)

        return MixedAddRemove400Scenario(
            memberships = memberships,
            graphErrorGroupRefs = graphErrorGroupRefs,
            expectedStatuses = expectedStatuses.filterValues { it > 0 },
        )
    }

    private fun MutableMap<EntraStatus, Int>.increment(status: EntraStatus) {
        this[status] = getValue(status) + 1
    }

    private fun producerProperties(): Properties =
        Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer::class.java)
            put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false)
            put(ProducerConfig.ACKS_CONFIG, "all")
            put(ProducerConfig.LINGER_MS_CONFIG, "10")
            put(ProducerConfig.BATCH_SIZE_CONFIG, "65536")
        }

    private fun consumerProperties(): Properties =
        Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG, "membership-load-result-${UUID.randomUUID()}")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer::class.java)
        }

    private fun graphResponseDelay(): Duration = Duration.ofMillis(GRAPH_RESPONSE_DELAY_MS)

    @TestConfiguration
    class KafkaTopicConfiguration {
        @Bean
        fun membershipInputTopic(): NewTopic = NewTopic(INPUT_TOPIC, TOPIC_PARTITIONS, 1)

        @Bean
        fun membershipResultTopic(): NewTopic = NewTopic(RESULT_TOPIC, RESULT_TOPIC_PARTITIONS, 1)
    }

    @TestConfiguration
    class GraphClientConfiguration {
        @Bean
        fun graphCallCounter(): AtomicInteger = AtomicInteger()

        @Bean
        @Primary
        fun loadTestGraphServiceClient(graphCallCounter: AtomicInteger): GraphServiceClient {
            val requestAdapter = graphRequestAdapter()
            return FakeGraphServiceClient(
                requestAdapter = requestAdapter,
                batchRequestBuilder = FakeBatchRequestBuilder(requestAdapter, graphCallCounter),
            )
        }

        private fun graphRequestAdapter(): RequestAdapter {
            val requestAdapter = mockk<RequestAdapter>(relaxed = true)
            val writerFactory = mockk<SerializationWriterFactory>()
            val writer = mockk<SerializationWriter>(relaxed = true)

            every { requestAdapter.serializationWriterFactory } returns writerFactory
            every { writerFactory.getSerializationWriter(any()) } returns writer
            every { writer.serializedContent } returns ByteArrayInputStream("""{}""".toByteArray())
            every { requestAdapter.convertToNativeRequest<Request>(any()) } answers {
                val requestInformation = firstArg<RequestInformation>()
                val method = requireNotNull(requestInformation.httpMethod).name
                val body =
                    if (method == "POST" || method == "PATCH" || method == "PUT") {
                        ByteArray(0).toRequestBody(null)
                    } else {
                        null
                    }
                val rawUrl = requireNotNull(requestInformation.uri).toString()
                val absoluteUrl =
                    if (rawUrl.startsWith("http://") || rawUrl.startsWith("https://")) {
                        rawUrl
                    } else {
                        "https://graph.microsoft.com/v1.0$rawUrl"
                    }

                Request
                    .Builder()
                    .url(absoluteUrl)
                    .method(method, body)
                    .build()
            }

            return requestAdapter
        }

        private class FakeGraphServiceClient(
            requestAdapter: RequestAdapter,
            private val batchRequestBuilder: BatchRequestBuilder,
        ) : GraphServiceClient(requestAdapter) {
            override fun getBatchRequestBuilder(): BatchRequestBuilder = batchRequestBuilder
        }

        private class FakeBatchRequestBuilder(
            requestAdapter: RequestAdapter,
            private val graphCallCounter: AtomicInteger,
        ) : BatchRequestBuilder(requestAdapter) {
            override fun post(
                batchRequestContent: BatchRequestContent,
                errorMappings: MutableMap<String, ParsableFactory<out Parsable>>?,
            ): BatchResponseContent {
                graphCallCounter.incrementAndGet()
                Thread.sleep(GRAPH_RESPONSE_DELAY_MS)
                return batchResponse(batchRequestContent)
            }
        }

        companion object {
            private val graphBadRequestGroupRefs = ConcurrentHashMap.newKeySet<String>()

            fun reset() {
                graphBadRequestGroupRefs.clear()
            }

            fun returnBadRequestForGroups(groupRefs: Set<String>) {
                graphBadRequestGroupRefs += groupRefs
            }

            fun batchResponse(batchRequestContent: BatchRequestContent): BatchResponseContent {
                val responseJson =
                    batchRequestContent.batchRequestSteps.entries.joinToString(
                        separator = ",",
                        prefix = """{"responses":[""",
                        postfix = "]}",
                    ) { (requestId, step) ->
                        val status = graphStatus(step.request.url.toString())
                        """{"id":"$requestId","status":$status}"""
                    }

                val response =
                    Response
                        .Builder()
                        .request(
                            Request
                                .Builder()
                                .url("https://graph.microsoft.com/v1.0/\$batch")
                                .build(),
                        ).protocol(Protocol.HTTP_1_1)
                        .code(200)
                        .message("OK")
                        .body(responseJson.toResponseBody("application/json".toMediaType()))
                        .build()

                return BatchResponseContent(response)
            }

            private fun graphStatus(url: String): Int =
                if (graphBadRequestGroupRefs.any { groupRef -> url.contains(groupRef) }) {
                    400
                } else {
                    204
                }
        }
    }

    companion object {
        private val objectMapper = ObjectMapper()
        private const val TOPIC_PARTITIONS = 12
        private const val RESULT_TOPIC_PARTITIONS = 1
        private const val DB_BATCH_SIZE = 1000
        private const val PERFORMANCE_TEST_MESSAGE_COUNT = 100000
        private const val CORRECTNESS_TEST_MESSAGE_COUNT = 1000
        private const val GRAPH_RESPONSE_DELAY_MS = 100L
        private val CORRECTNESS_TEST_TIMEOUT: Duration = Duration.ofMinutes(4)
        private val PERFORMANCE_TEST_TIMEOUT: Duration = Duration.ofMinutes(10)
        private const val INPUT_TOPIC = "fintlabs-no.kontroll.event.kontroll-resource-group-membership-device"
        private const val RESULT_TOPIC = "fintlabs-no.kontroll.event.entra-device-group-membership"

        @Container
        @JvmStatic
        val kafka: KafkaContainer =
            KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))

        @Container
        @JvmStatic
        val postgres: PostgreSQLContainer<*> =
            PostgreSQLContainer(DockerImageName.parse("postgres:16-alpine"))
                .withDatabaseName("msgraphgateway")
                .withUsername("test")
                .withPassword("test")

        @JvmStatic
        @DynamicPropertySource
        fun registerContainerProperties(registry: DynamicPropertyRegistry) {
            registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers)
            registry.add("fint.database.url", postgres::getJdbcUrl)
            registry.add("fint.database.username", postgres::getUsername)
            registry.add("fint.database.password", postgres::getPassword)
        }

        private data class MixedAddRemove400Scenario(
            val memberships: List<DeviceResourceGroupMembership>,
            val graphErrorGroupRefs: Set<String>,
            val expectedStatuses: Map<EntraStatus, Int>,
        )
    }
}
