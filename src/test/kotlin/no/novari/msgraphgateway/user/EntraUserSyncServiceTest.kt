package no.novari.msgraphgateway.user

import com.microsoft.graph.models.User
import com.microsoft.kiota.store.InMemoryBackingStore
import io.mockk.clearAllMocks
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import no.novari.msgraphgateway.config.ConfigUser
import no.novari.msgraphgateway.entra.Checksum
import no.novari.msgraphgateway.entra.ChecksumService
import no.novari.msgraphgateway.kafka.UserExternalProducerService
import no.novari.msgraphgateway.kafka.UserProducerService
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class EntraUserSyncServiceTest {
    private lateinit var userRepository: UserRepository
    private lateinit var userExternalRepository: UserExternalRepository
    private lateinit var checksumService: ChecksumService
    private lateinit var producer: UserProducerService
    private lateinit var externalProducer: UserExternalProducerService
    private lateinit var configUser: ConfigUser
    private lateinit var service: EntraUserSyncService

    @Test
    fun processPagePublishesOnlyChangedUsersWhenRepublishAllIsFalse() =
        runTest {
            val firstId = UUID.randomUUID()
            val secondId = UUID.randomUUID()
            val users = listOf(memberUser(firstId), memberUser(secondId))

            every { userRepository.batchUpsertReturningChanged(any()) } returns setOf(firstId)

            val published =
                service.processPage(
                    users = users,
                    notSeenIncremented = mutableSetOf(),
                    republishAll = false,
                )

            assertEquals(1, published)
            verify(exactly = 1) { userRepository.batchUpsertReturningChanged(match { it.size == 2 }) }
            verify(exactly = 0) { userRepository.batchUpsert(any()) }
            coVerify(exactly = 1) { producer.publish(any()) }
            coVerify(exactly = 0) { externalProducer.publish(any()) }
        }

    @Test
    fun processPageRepublishesAllUsersWhenRepublishAllIsTrue() =
        runTest {
            val firstId = UUID.randomUUID()
            val secondId = UUID.randomUUID()
            val users = listOf(memberUser(firstId), memberUser(secondId))

            val published =
                service.processPage(
                    users = users,
                    notSeenIncremented = mutableSetOf(),
                    republishAll = true,
                )

            assertEquals(2, published)
            verify(exactly = 1) { userRepository.batchUpsert(match { it.size == 2 }) }
            verify(exactly = 0) { userRepository.batchUpsertReturningChanged(any()) }
            coVerify(exactly = 2) { producer.publish(any()) }
        }

    @Test
    fun processPageDeduplicatesUsersWithinSamePage() =
        runTest {
            val duplicateId = UUID.randomUUID()
            val users = listOf(memberUser(duplicateId), memberUser(duplicateId))

            val published =
                service.processPage(
                    users = users,
                    notSeenIncremented = mutableSetOf(),
                    republishAll = true,
                )

            assertEquals(1, published)
            verify(
                exactly = 1,
            ) { userRepository.batchUpsert(match { it.size == 1 && it.first().objectId == duplicateId }) }
            coVerify(exactly = 1) { producer.publish(any()) }
        }

    @Test
    fun processPageRoutesExternalUsersToExternalRepositoryAndProducer() =
        runTest {
            configUser.enableExternalUsers = true
            configUser.externaluserattribute = "externalFlag"
            configUser.externaluservalue = "yes"

            val normalId = UUID.randomUUID()
            val externalId = UUID.randomUUID()
            val users =
                listOf(
                    memberUser(normalId),
                    memberUser(
                        externalId,
                        additionalData = mutableMapOf("externalFlag" to "yes"),
                        backingStoreValues = mapOf("externalFlag" to "yes"),
                    ),
                )

            val published =
                service.processPage(
                    users = users,
                    notSeenIncremented = mutableSetOf(),
                    republishAll = true,
                )

            assertEquals(2, published)
            verify(exactly = 1) {
                userRepository.batchUpsert(
                    match {
                        it.map { row ->
                            row.objectId
                        } == listOf(normalId)
                    },
                )
            }
            verify(exactly = 1) {
                userExternalRepository.batchUpsert(match { it.map { row -> row.objectId } == listOf(externalId) })
            }
            coVerify(exactly = 1) { producer.publish(any()) }
            coVerify(exactly = 1) { externalProducer.publish(any()) }
        }

    @Test
    fun processPageMarksRemovedUserOnlyOncePerRun() =
        runTest {
            val removedId = UUID.randomUUID()
            val removedUsers = listOf(removedUser(removedId), removedUser(removedId))

            every { userRepository.existsById(removedId) } returns true

            service.processPage(
                users = removedUsers,
                notSeenIncremented = mutableSetOf(),
                republishAll = false,
            )

            verify(exactly = 1) { userRepository.incrementNotSeenCount(listOf(removedId)) }
            verify(exactly = 0) { userExternalRepository.incrementNotSeenCount(any()) }
        }

    @BeforeEach
    fun beforeEach() {
        userRepository = mockk(relaxed = true)
        userExternalRepository = mockk(relaxed = true)
        checksumService = mockk(relaxed = true)
        producer = mockk(relaxed = true)
        externalProducer = mockk(relaxed = true)
        configUser =
            ConfigUser().apply {
                enableExternalUsers = false
                useSameIdNumAttribute = false
            }

        every { checksumService.checksum(any()) } returns Checksum(byteArrayOf(1, 2, 3))
        every { userRepository.batchUpsertReturningChanged(any()) } returns emptySet()
        every { userRepository.batchUpsert(any()) } returns Unit
        every { userExternalRepository.batchUpsertReturningChanged(any()) } returns emptySet()
        every { userExternalRepository.batchUpsert(any()) } returns Unit
        every { userRepository.existsById(any()) } returns false
        every { userExternalRepository.existsById(any()) } returns false

        coEvery { producer.publish(any()) } returns Unit
        coEvery { externalProducer.publish(any()) } returns Unit

        service =
            EntraUserSyncService(
                userRepository = userRepository,
                userExternalRepository = userExternalRepository,
                checksumService = checksumService,
                producer = producer,
                externalProducer = externalProducer,
                configUser = configUser,
            )
    }

    @AfterEach
    fun tearDown() {
        clearAllMocks()
    }

    private fun memberUser(
        id: UUID,
        additionalData: MutableMap<String, Any> = mutableMapOf(),
        backingStoreValues: Map<String, Any> = emptyMap(),
    ): User =
        mockk(relaxed = true) {
            val backingStore =
                InMemoryBackingStore().apply {
                    backingStoreValues.forEach { (key, value) -> set(key, value) }
                }
            every { this@mockk.id } returns id.toString()
            every { userType } returns "Member"
            every { mail } returns "$id@example.org"
            every { userPrincipalName } returns "$id@example.org"
            every { accountEnabled } returns true
            every { this@mockk.additionalData } returns additionalData
            every { this@mockk.backingStore } returns backingStore
        }

    private fun removedUser(id: UUID): User =
        mockk(relaxed = true) {
            every { this@mockk.id } returns id.toString()
            every { additionalData } returns mutableMapOf("@removed" to mapOf("reason" to "deleted"))
        }
}
