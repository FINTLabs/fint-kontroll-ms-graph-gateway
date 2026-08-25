package no.novari.msgraphgateway.membership.user

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
import no.novari.msgraphgateway.dto.EntraUserMembershipDto
import no.novari.msgraphgateway.entra.EntraStatus
import no.novari.msgraphgateway.kafka.OperationType
import no.novari.msgraphgateway.kafka.membership.EntraUserMembershipProducer
import no.novari.msgraphgateway.membership.MembershipProcessingProperties
import no.novari.msgraphgateway.repository.user.UserMembershipEntity
import no.novari.msgraphgateway.repository.user.UserMembershipEntityRepository
import no.novari.msgraphgateway.repository.user.UserMembershipId
import no.novari.msgraphgateway.services.member.UserMembershipService
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

class UserMembershipServiceTest {
    private lateinit var graphServiceClient: GraphServiceClient
    private lateinit var requestAdapter: RequestAdapter
    private lateinit var batchRequestBuilder: BatchRequestBuilder
    private lateinit var entraUserMembershipProducer: EntraUserMembershipProducer
    private lateinit var userMembershipEntityRepository: UserMembershipEntityRepository
    private lateinit var service: UserMembershipService

    @Test
    fun processKontrollMembershipBatchPublishesErrorForInvalidMembershipIds() {
        val messageKey = "invalid-membership"
        val membership =
            UserResourceGroupMembership(
                operation = OperationType.ADD,
                entraGroupRef = "not-a-uuid",
                userGroupRef = UUID.randomUUID().toString(),
            )

        service.processKontrollMembershipBatch(listOf(record(messageKey, membership)))

        verify(exactly = 0) { userMembershipEntityRepository.findAllByIds(any()) }
        verify(exactly = 0) { userMembershipEntityRepository.saveAll(any()) }
        verify(exactly = 1) {
            entraUserMembershipProducer.publish(
                messageKey,
                EntraUserMembershipDto(
                    code = EntraStatus.ERROR,
                    entraGroupRef = membership.entraGroupRef,
                    entraUserRef = membership.userGroupRef,
                ),
            )
        }
    }

    @Test
    fun processKontrollMembershipBatchSkipsDuplicateAddAndPublishesNoChanges() {
        val userRef = UUID.randomUUID()
        val groupRef = UUID.randomUUID()
        val membership =
            UserResourceGroupMembership(
                operation = OperationType.ADD,
                entraGroupRef = groupRef.toString(),
                userGroupRef = userRef.toString(),
            )
        val existing =
            UserMembershipEntity(
                id = UserMembershipId(userRef, groupRef),
                status = EntraStatus.ADDED,
                createdAt = OffsetDateTime.parse("2026-04-29T10:00:00Z"),
                lastUpdatedAt = OffsetDateTime.parse("2026-04-29T10:00:00Z"),
            )
        val membershipId = UserMembershipId(userRef, groupRef)
        val savedSlot = slot<Collection<UserMembershipEntity>>()

        every {
            userMembershipEntityRepository.findAllByIds(listOf(membershipId))
        } returns mapOf(membershipId to existing)
        every { userMembershipEntityRepository.saveAll(capture(savedSlot)) } returns Unit

        service.processKontrollMembershipBatch(listOf(record("duplicate-add", membership)))

        assertEquals(1, savedSlot.captured.size)
        assertEquals(EntraStatus.ADDED, savedSlot.captured.single().status)
        assertEquals(existing.createdAt, savedSlot.captured.single().createdAt)
        verify(exactly = 1) {
            userMembershipEntityRepository.findAllByIds(listOf(membershipId))
        }
        verify(exactly = 1) { userMembershipEntityRepository.saveAll(any()) }
        verify(exactly = 1) {
            entraUserMembershipProducer.publish(
                "duplicate-add",
                EntraUserMembershipDto(
                    code = EntraStatus.NO_CHANGES,
                    entraGroupRef = groupRef.toString(),
                    entraUserRef = userRef.toString(),
                ),
            )
        }
        verify(exactly = 0) { graphServiceClient.batchRequestBuilder }
    }

    @ParameterizedTest
    @MethodSource("graphStatusCases")
    fun processKontrollMembershipBatchMapsGraphStatuses(testCase: GraphStatusTestCase) {
        val userRef = UUID.randomUUID()
        val groupRef = UUID.randomUUID()
        val membership =
            UserResourceGroupMembership(
                operation = testCase.operation,
                entraGroupRef = groupRef.toString(),
                userGroupRef = userRef.toString(),
            )
        val savedSlot = slot<Collection<UserMembershipEntity>>()

        every { userMembershipEntityRepository.saveAll(capture(savedSlot)) } returns Unit
        everyGraphBatchResponse(testCase.statusCode, testCase.error)

        service.processKontrollMembershipBatch(listOf(record("graph-status", membership)))

        assertEquals(1, savedSlot.captured.size)
        assertEquals(testCase.expectedPersistedStatus, savedSlot.captured.single().status)
        verify(exactly = 1) {
            entraUserMembershipProducer.publish(
                "graph-status",
                EntraUserMembershipDto(
                    code = testCase.expectedPublishedStatus,
                    entraGroupRef = groupRef.toString(),
                    entraUserRef = userRef.toString(),
                ),
            )
        }
    }

    @Test
    fun deleteAllMembershipsDeletesAllRows() {
        every { userMembershipEntityRepository.deleteAll() } returns 11

        val deletedCount = service.deleteAllMemberships()

        assertEquals(11, deletedCount)
        verify(exactly = 1) { userMembershipEntityRepository.deleteAll() }
    }

    @Test
    fun deleteMembershipsUpdatedBeforeDeletesOlderRows() {
        val cutoff = OffsetDateTime.parse("2026-05-01T00:00:00Z")
        every { userMembershipEntityRepository.deleteLastUpdatedBefore(cutoff) } returns 8

        val deletedCount = service.deleteMembershipsUpdatedBefore(cutoff)

        assertEquals(8, deletedCount)
        verify(exactly = 1) { userMembershipEntityRepository.deleteLastUpdatedBefore(cutoff) }
    }

    @BeforeEach
    fun beforeEach() {
        graphServiceClient = mockk(relaxed = true)
        requestAdapter = mockk(relaxed = true)
        batchRequestBuilder = mockk(relaxed = true)
        entraUserMembershipProducer = mockk(relaxed = true)
        userMembershipEntityRepository = mockk(relaxed = true)

        every { userMembershipEntityRepository.findAllByIds(any()) } returns emptyMap()
        every { userMembershipEntityRepository.saveAll(any()) } returns Unit
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
                urlTemplate = "https://graph.microsoft.com/v1.0/groups/group/members/user/\$ref"
            }

        service =
            UserMembershipService(
                graphServiceClient = graphServiceClient,
                entraMembershipProducer = entraUserMembershipProducer,
                userMembershipEntityRepository = userMembershipEntityRepository,
                properties =
                    MembershipProcessingProperties(
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
        value: UserResourceGroupMembership,
    ): ConsumerRecord<String, UserResourceGroupMembership> =
        ConsumerRecord("kontroll-resource-group-membership-user", 0, 0, key, value)

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
                    error = "User not found",
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
