package no.novari.msgraphgateway.user

import com.microsoft.graph.models.User
import com.microsoft.kiota.store.InMemoryBackingStore
import io.mockk.Runs
import io.mockk.clearAllMocks
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import kotlinx.coroutines.test.runTest
import no.novari.msgraphgateway.config.ConfigUser
import no.novari.msgraphgateway.kafka.user.UserExternalProducerService
import no.novari.msgraphgateway.kafka.user.UserProducerService
import no.novari.msgraphgateway.repository.user.UserExternalRepository
import no.novari.msgraphgateway.repository.user.UserRepository
import no.novari.msgraphgateway.services.Checksum
import no.novari.msgraphgateway.services.ChecksumService
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

            assertEquals(EntraUserSyncService.UserSyncPageResult(publishedUsers = 1, removedUsers = 0), published)
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

            assertEquals(EntraUserSyncService.UserSyncPageResult(publishedUsers = 2, removedUsers = 0), published)
            verify(exactly = 1) { userRepository.batchUpsert(match { it.size == 2 }) }
            verify(exactly = 0) { userRepository.batchUpsertReturningChanged(any()) }
            coVerify(exactly = 2) { producer.publish(any()) }
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

            assertEquals(EntraUserSyncService.UserSyncPageResult(publishedUsers = 2, removedUsers = 0), published)
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

    @Test
    fun processPageCalculatesChecksumBeforeImportingUsers() =
        runTest {
            val userId = UUID.randomUUID()
            val users = listOf(memberUser(userId))

            every { userRepository.batchUpsertReturningChanged(any()) } returns setOf(userId)

            service.processPage(
                users = users,
                notSeenIncremented = mutableSetOf(),
                republishAll = false,
            )

            verifyOrder {
                checksumService.checksum(any())
                userRepository.batchUpsertReturningChanged(any())
            }

            coVerify(exactly = 1) {
                producer.publish(any())
            }
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

    @Test
    fun `processPage returns removed count per page and total can be accumulated`() =
        runTest {
            coEvery { userRepository.existsById(any()) } returns true
            coEvery { userRepository.incrementNotSeenCount(any()) } just Runs

            val notSeenIncremented = mutableSetOf<UUID>()

            val page1 = removedUsers(3)
            val page2 = removedUsers(4)
            val page3 = removedUsers(5)

            val result1 = service.processPage(page1, notSeenIncremented, republishAll = false)
            val result2 = service.processPage(page2, notSeenIncremented, republishAll = false)
            val result3 = service.processPage(page3, notSeenIncremented, republishAll = false)

            assertEquals(3, result1.removedUsers)
            assertEquals(4, result2.removedUsers)
            assertEquals(5, result3.removedUsers)

            val totalRemoved =
                result1.removedUsers +
                    result2.removedUsers +
                    result3.removedUsers

            assertEquals(12, totalRemoved)

            assertEquals(0, result1.publishedUsers)
            assertEquals(0, result2.publishedUsers)
            assertEquals(0, result3.publishedUsers)

            coVerify(exactly = 12) {
                userRepository.existsById(any())
            }

            coVerify(exactly = 12) {
                userRepository.incrementNotSeenCount(any())
            }
        }

    @AfterEach
    fun tearDown() {
        clearAllMocks()
    }

    private fun removedUsers(count: Int): List<User> =
        (1..count).map {
            User().apply {
                id = UUID.randomUUID().toString()
                additionalData["@removed"] = mapOf<String, Any>()
            }
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
