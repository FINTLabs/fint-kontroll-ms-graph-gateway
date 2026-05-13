package no.novari.msgraphgateway.membership.device

import com.microsoft.graph.core.content.BatchRequestContent
import com.microsoft.graph.core.content.BatchResponseContent
import com.microsoft.graph.core.requests.BatchRequestBuilder
import com.microsoft.graph.serviceclient.GraphServiceClient
import com.microsoft.kiota.HttpMethod
import com.microsoft.kiota.RequestAdapter
import com.microsoft.kiota.RequestInformation
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.Response
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.time.OffsetDateTime
import java.util.UUID
import java.util.stream.Stream

class MembershipServiceTest {
    private lateinit var graphServiceClient: GraphServiceClient
    private lateinit var requestAdapter: RequestAdapter
    private lateinit var batchRequestBuilder: BatchRequestBuilder
    private lateinit var entraMembershipProducer: EntraMembershipProducer
    private lateinit var deviceMembershipEntityRepository: DeviceMembershipEntityRepository
    private lateinit var service: MembershipService

    @Test
    fun processKontrollMembershipBatchPublishesErrorForInvalidMembershipIds() {
        val messageKey = "invalid-membership"
        val membership =
            DeviceResourceGroupMembership(
                operation = OperationType.ADD,
                entraGroupRef = "not-a-uuid",
                entraDeviceRef = UUID.randomUUID().toString(),
            )

        service.processKontrollMembershipBatch(listOf(record(messageKey, membership)))

        verify(exactly = 0) { deviceMembershipEntityRepository.findAllByIds(any()) }
        verify(exactly = 0) { deviceMembershipEntityRepository.saveAll(any()) }
        verify(exactly = 1) {
            entraMembershipProducer.publish(
                messageKey,
                EntraDeviceMembership(
                    code = EntraStatus.ERROR,
                    entraGroupRef = membership.entraGroupRef,
                    entraDeviceRef = membership.entraDeviceRef,
                ),
            )
        }
    }

    @Test
    fun processKontrollMembershipBatchSkipsDuplicateAddAndPublishesNoChanges() {
        val deviceRef = UUID.randomUUID()
        val groupRef = UUID.randomUUID()
        val membership =
            DeviceResourceGroupMembership(
                operation = OperationType.ADD,
                entraGroupRef = groupRef.toString(),
                entraDeviceRef = deviceRef.toString(),
            )
        val existing =
            DeviceMembershipEntity(
                id = DeviceMembershipId(deviceRef, groupRef),
                status = EntraStatus.ADDED,
                createdAt = OffsetDateTime.parse("2026-04-29T10:00:00Z"),
                lastUpdatedAt = OffsetDateTime.parse("2026-04-29T10:00:00Z"),
            )
        val membershipId = DeviceMembershipId(deviceRef, groupRef)
        val savedSlot = slot<Collection<DeviceMembershipEntity>>()

        every {
            deviceMembershipEntityRepository.findAllByIds(listOf(membershipId))
        } returns mapOf(membershipId to existing)
        every {
            deviceMembershipEntityRepository.saveAll(
                capture(savedSlot),
            )
        } returns Unit

        service.processKontrollMembershipBatch(listOf(record("duplicate-add", membership)))

        assertEquals(1, savedSlot.captured.size)
        assertEquals(EntraStatus.ADDED, savedSlot.captured.single().status)
        assertEquals(existing.createdAt, savedSlot.captured.single().createdAt)
        verify(exactly = 1) {
            deviceMembershipEntityRepository.findAllByIds(listOf(membershipId))
        }
        verify(exactly = 1) { deviceMembershipEntityRepository.saveAll(any()) }
        verify(exactly = 1) {
            entraMembershipProducer.publish(
                "duplicate-add",
                EntraDeviceMembership(
                    code = EntraStatus.NO_CHANGES,
                    entraGroupRef = groupRef.toString(),
                    entraDeviceRef = deviceRef.toString(),
                ),
            )
        }
        verify(exactly = 0) { graphServiceClient.batchRequestBuilder }
    }

    @ParameterizedTest
    @MethodSource("graphStatusCases")
    fun processKontrollMembershipBatchMapsGraphStatuses(testCase: GraphStatusTestCase) {
        val deviceRef = UUID.randomUUID()
        val groupRef = UUID.randomUUID()
        val membership =
            DeviceResourceGroupMembership(
                operation = testCase.operation,
                entraGroupRef = groupRef.toString(),
                entraDeviceRef = deviceRef.toString(),
            )
        val savedSlot = slot<Collection<DeviceMembershipEntity>>()

        every { deviceMembershipEntityRepository.saveAll(capture(savedSlot)) } returns Unit
        everyGraphBatchResponse(testCase.statusCode, testCase.error)

        service.processKontrollMembershipBatch(listOf(record("graph-status", membership)))

        assertEquals(1, savedSlot.captured.size)
        assertEquals(testCase.expectedPersistedStatus, savedSlot.captured.single().status)
        verify(exactly = 1) {
            entraMembershipProducer.publish(
                "graph-status",
                EntraDeviceMembership(
                    code = testCase.expectedPublishedStatus,
                    entraGroupRef = groupRef.toString(),
                    entraDeviceRef = deviceRef.toString(),
                ),
            )
        }
    }

    @Test
    fun deleteAllMembershipsDeletesAllRows() {
        every { deviceMembershipEntityRepository.deleteAll() } returns 11

        val deletedCount = service.deleteAllMemberships()

        assertEquals(11, deletedCount)
        verify(exactly = 1) { deviceMembershipEntityRepository.deleteAll() }
    }

    @Test
    fun deleteMembershipsUpdatedBeforeDeletesOlderRows() {
        val cutoff = OffsetDateTime.parse("2026-05-01T00:00:00Z")
        every { deviceMembershipEntityRepository.deleteLastUpdatedBefore(cutoff) } returns 8

        val deletedCount = service.deleteMembershipsUpdatedBefore(cutoff)

        assertEquals(8, deletedCount)
        verify(exactly = 1) { deviceMembershipEntityRepository.deleteLastUpdatedBefore(cutoff) }
    }

    @BeforeEach
    fun beforeEach() {
        graphServiceClient = mockk(relaxed = true)
        requestAdapter = mockk(relaxed = true)
        batchRequestBuilder = mockk(relaxed = true)
        entraMembershipProducer = mockk(relaxed = true)
        deviceMembershipEntityRepository = mockk(relaxed = true)

        every { deviceMembershipEntityRepository.findAllByIds(any()) } returns emptyMap()
        every { deviceMembershipEntityRepository.saveAll(any()) } returns Unit
        every { graphServiceClient.requestAdapter } returns requestAdapter
        every { graphServiceClient.batchRequestBuilder } returns batchRequestBuilder
        every { requestAdapter.convertToNativeRequest<Request>(any()) } returns
            Request
                .Builder()
                .url("https://graph.microsoft.com/v1.0/groups/group/members/\$ref")
                .get()
                .build()
        every {
            graphServiceClient
                .groups()
                .byGroupId(any())
                .members()
                .ref()
                .toPostRequestInformation(any())
        } returns
            RequestInformation().apply {
                httpMethod = HttpMethod.POST
                urlTemplate = "https://graph.microsoft.com/v1.0/groups/group/members/\$ref"
            }
        every {
            graphServiceClient
                .groups()
                .byGroupId(any())
                .members()
                .byDirectoryObjectId(any())
                .ref()
                .toDeleteRequestInformation()
        } returns
            RequestInformation().apply {
                httpMethod = HttpMethod.DELETE
                urlTemplate = "https://graph.microsoft.com/v1.0/groups/group/members/device/\$ref"
            }

        service =
            MembershipService(
                graphServiceClient = graphServiceClient,
                entraMembershipProducer = entraMembershipProducer,
                deviceMembershipEntityRepository = deviceMembershipEntityRepository,
                properties =
                    DeviceMembershipProcessingProperties(
                        consumerConcurrency = 1,
                        consumerMaxPollRecords = 100,
                        graphMaxConcurrentCalls = 3,
                        graphBatchSize = 20,
                        resultTopicPartitions = 1,
                        directoryObjectsBaseUrl = "testUrl",
                    ),
            )
    }

    @AfterEach
    fun tearDown() {
        clearAllMocks()
    }

    private fun record(
        key: String,
        value: DeviceResourceGroupMembership,
    ): ConsumerRecord<String, DeviceResourceGroupMembership> =
        ConsumerRecord("kontroll-resource-group-membership-device", 0, 0, key, value)

    private fun everyGraphBatchResponse(
        statusCode: Int,
        error: String?,
    ) {
        every { batchRequestBuilder.post(any<BatchRequestContent>(), null) } answers {
            val batchRequestContent = firstArg<BatchRequestContent>()
            val responseStatusCodes = batchRequestContent.batchRequestSteps.keys.associateWith { statusCode }
            val batchResponse = mockk<BatchResponseContent>()

            every { batchResponse.responsesStatusCode } returns responseStatusCodes
            every { batchResponse.getResponseById(any()) } answers {
                Response
                    .Builder()
                    .request(
                        Request
                            .Builder()
                            .url("https://graph.microsoft.com/v1.0/\$batch")
                            .build(),
                    ).protocol(Protocol.HTTP_1_1)
                    .code(statusCode)
                    .message(error ?: "Graph status $statusCode")
                    .build()
            }

            batchResponse
        }
    }

    companion object {
        @JvmStatic
        fun graphStatusCases(): Stream<GraphStatusTestCase> =
            Stream.of(
                GraphStatusTestCase(
                    operation = OperationType.ADD,
                    statusCode = 204,
                    error = null,
                    expectedPublishedStatus = EntraStatus.ADDED,
                    expectedPersistedStatus = EntraStatus.ADDED,
                ),
                GraphStatusTestCase(
                    operation = OperationType.ADD,
                    statusCode = 400,
                    error = "Bad request",
                    expectedPublishedStatus = EntraStatus.ERROR,
                    expectedPersistedStatus = EntraStatus.ERROR,
                ),
                GraphStatusTestCase(
                    operation = OperationType.ADD,
                    statusCode = 400,
                    error = "One or more added object references already exist",
                    expectedPublishedStatus = EntraStatus.NO_CHANGES,
                    expectedPersistedStatus = EntraStatus.ADDED,
                ),
                GraphStatusTestCase(
                    operation = OperationType.ADD,
                    statusCode = 404,
                    error = "Device not found",
                    expectedPublishedStatus = EntraStatus.ERROR,
                    expectedPersistedStatus = EntraStatus.ERROR,
                ),
                GraphStatusTestCase(
                    operation = OperationType.ADD,
                    statusCode = 300,
                    error = "Multiple choices",
                    expectedPublishedStatus = EntraStatus.FAILED,
                    expectedPersistedStatus = EntraStatus.FAILED,
                ),
                GraphStatusTestCase(
                    operation = OperationType.REMOVE,
                    statusCode = 204,
                    error = null,
                    expectedPublishedStatus = EntraStatus.REMOVED,
                    expectedPersistedStatus = EntraStatus.REMOVED,
                ),
                GraphStatusTestCase(
                    operation = OperationType.REMOVE,
                    statusCode = 404,
                    error = "Membership not found",
                    expectedPublishedStatus = EntraStatus.REMOVED,
                    expectedPersistedStatus = EntraStatus.REMOVED,
                ),
                GraphStatusTestCase(
                    operation = OperationType.REMOVE,
                    statusCode = 400,
                    error = "Bad request",
                    expectedPublishedStatus = EntraStatus.FAILED,
                    expectedPersistedStatus = EntraStatus.FAILED,
                ),
                GraphStatusTestCase(
                    operation = OperationType.REMOVE,
                    statusCode = 300,
                    error = "Multiple choices",
                    expectedPublishedStatus = EntraStatus.FAILED,
                    expectedPersistedStatus = EntraStatus.FAILED,
                ),
            )
    }

    data class GraphStatusTestCase(
        val operation: OperationType,
        val statusCode: Int,
        val error: String?,
        val expectedPublishedStatus: EntraStatus,
        val expectedPersistedStatus: EntraStatus,
    )
}
