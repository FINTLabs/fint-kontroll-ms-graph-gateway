package no.novari.msgraphgateway.user

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import com.microsoft.graph.models.User
import com.microsoft.graph.serviceclient.GraphServiceClient
import com.microsoft.graph.users.UsersRequestBuilder
import com.microsoft.graph.users.count.CountRequestBuilder
import com.microsoft.graph.users.delta.DeltaGetResponse
import com.microsoft.graph.users.delta.DeltaRequestBuilder
import com.microsoft.kiota.ApiException
import io.mockk.Runs
import io.mockk.clearAllMocks
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import no.novari.msgraphgateway.config.ConfigUser
import no.novari.msgraphgateway.entra.DeltaLinkStore
import no.novari.msgraphgateway.repository.user.UserExternalRepository
import no.novari.msgraphgateway.repository.user.UserRepository
import no.novari.msgraphgateway.services.user.EntraUserSyncService
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory

@ExperimentalCoroutinesApi
class MsGraphUserTest {
    private lateinit var configUser: ConfigUser
    private lateinit var deltaLinkStore: DeltaLinkStore
    private lateinit var graphServiceClient: GraphServiceClient
    private lateinit var usersRb: UsersRequestBuilder
    private lateinit var deltaRb: DeltaRequestBuilder
    private lateinit var countRb: CountRequestBuilder
    private lateinit var entraUserSyncService: EntraUserSyncService
    private lateinit var userRepository: UserRepository
    private lateinit var userExternalRepository: UserExternalRepository
    private lateinit var service: MsGraphUser

    @Test
    fun invalidDeltaLinkResetsAndStoresNewDeltaLink() =
        runBlocking {
            val invalid = mockk<ApiException>(relaxed = true)
            every { invalid.responseStatusCode } returns 400
            every { invalid.message } returns "Badly formed token"
            every { deltaRb.withUrl("old_bad_deltalink") } returns deltaRb

            val firstPage = mockDeltaPage(deltaLink = "new_deltalink")
            every { deltaRb.get(any()) } throws invalid andThen firstPage

            val sut = createMsGraphUser()
            setPrivateField(sut, "userDeltaLink", "old_bad_deltalink")

            sut.pullAllUsersDelta()

            verify(timeout = 2_000, exactly = 1) { deltaRb.withUrl("old_bad_deltalink") }
            verify(timeout = 2_000, exactly = 2) { deltaRb.get(any()) }
            verify(timeout = 2_000, exactly = 1) { deltaLinkStore.createOrUpdate("users", "") }
            verify(timeout = 2_000, exactly = 1) { deltaLinkStore.createOrUpdate("users", "new_deltalink") }
            assertEquals("new_deltalink", getPrivateField(sut, "userDeltaLink"))
        }

    @Test
    fun fullImportStopsWhenCountDeviationExceedsFivePercent() =
        runTest {
            every { configUser.acceptedDeviationPercent } returns 5
            every { userRepository.getCount() } returns 1000
            every { countRb.get(any()) } returns 900

            createMsGraphUser().startFullImport()

            verify(exactly = 0) { deltaRb.get(any()) }
            coVerify(exactly = 0) { entraUserSyncService.finishFullImport(any()) }
        }

    @Test
    fun fullImportStoresNewDeltaLink() =
        runTest {
            every { configUser.userAttributesDelta() } returns arrayOf("id", "displayName")
            every { configUser.staleAfterDays } returns 30
            every { configUser.acceptedDeviationPercent } returns null
            every { userRepository.getCount() } returns 0
            every { countRb.get(any()) } returns 100
            every { deltaRb.get(any()) } returns mockDeltaPage(deltaLink = "new_deltalink")

            val sut = createMsGraphUser()

            sut.startFullImport()

            verify(exactly = 1) { deltaRb.get(any()) }
            verify(exactly = 1) { deltaLinkStore.createOrUpdate("users", "new_deltalink") }
            coVerify(exactly = 1) { entraUserSyncService.finishFullImport(any()) }
            coVerify(exactly = 1) { entraUserSyncService.finishFullImportExternal(any()) }
            assertEquals("new_deltalink", getPrivateField(sut, "userDeltaLink"))
        }

    @Test
    fun requestFullImportQueuesSecondRunWhileFirstIsRunning(): Unit =
        runBlocking {
            val started = CompletableDeferred<Unit>()
            val finishFirstRun = CompletableDeferred<Unit>()
            val secondRunCompleted = CompletableDeferred<Unit>()
            var importCount = 0

            val msGraphUser = spyk(createMsGraphUser())
            coEvery { msGraphUser.startFullImport(any()) } coAnswers {
                importCount++
                if (importCount == 1) {
                    started.complete(Unit)
                    finishFirstRun.await()
                } else {
                    secondRunCompleted.complete(Unit)
                }
            }

            msGraphUser.requestFullImport(false)
            started.await()

            msGraphUser.requestFullImport(false)

            coVerify(timeout = 500, exactly = 1) { msGraphUser.startFullImport(false) }

            finishFirstRun.complete(Unit)
            secondRunCompleted.await()
            coVerify(timeout = 500, exactly = 2) { msGraphUser.startFullImport(false) }
        }

    @Test
    fun requestFullImportAfterDeltaRunPreservesRepublishFlag() =
        runBlocking {
            val deltaStarted = CompletableDeferred<Unit>()
            val releaseDelta = CompletableDeferred<Unit>()

            val firstPage = mockDeltaPage(deltaLink = "new_deltalink")
            every { deltaRb.get(any()) } answers {
                deltaStarted.complete(Unit)
                runBlocking { releaseDelta.await() }
                firstPage
            }

            val msGraphUser = spyk(createMsGraphUser())
            coEvery { msGraphUser.startFullImport(any()) } returns Unit

            msGraphUser.pullAllUsersDelta()
            deltaStarted.await()

            msGraphUser.requestFullImport(true)
            releaseDelta.complete(Unit)

            coVerify(timeout = 2_000, exactly = 1) { msGraphUser.startFullImport(true) }
        }

    @Test
    fun weeklyPublishUsersRequestsFullImportWithRepublishAll() =
        runBlocking {
            val msGraphUser = spyk(createMsGraphUser())
            coEvery { msGraphUser.startFullImport(any()) } returns Unit

            msGraphUser.weeklyPublishUsers()

            coVerify(timeout = 2_000, exactly = 1) { msGraphUser.startFullImport(true) }
        }

    @Test
    fun pullAllUsersDeltaDoesNotStartSecondRunWhileFirstIsRunning(): Unit =
        runBlocking {
            val firstRunStarted = CompletableDeferred<Unit>()
            val allowFirstRunToFinish = CompletableDeferred<Unit>()

            every { deltaRb.get(any()) } answers {
                firstRunStarted.complete(Unit)
                runBlocking { allowFirstRunToFinish.await() }
                mockDeltaPage(deltaLink = "new_deltalink")
            }

            val msGraphUser = createMsGraphUser()

            msGraphUser.pullAllUsersDelta()
            firstRunStarted.await()

            msGraphUser.pullAllUsersDelta()

            verify(timeout = 500, exactly = 1) { deltaRb.get(any()) }

            allowFirstRunToFinish.complete(Unit)
        }

    @Test
    fun `startFullImport processes all user pages`() =

        runTest {
            val logger =
                LoggerFactory.getLogger(MsGraphUser::class.java) as Logger

            val oldLevel = logger.level
            // Set log to debug for testing that all pages are processed
            logger.level = Level.DEBUG

            val page1 =
                DeltaGetResponse().apply {
                    value = listOf(User(), User(), User())
                    odataNextLink = "page-2"
                }

            val page2 =
                DeltaGetResponse().apply {
                    value = listOf(User(), User(), User(), User())
                    odataNextLink = "page-3"
                }

            val page3 =
                DeltaGetResponse().apply {
                    value = listOf(User(), User(), User(), User(), User())
                    odataDeltaLink = "delta-link"
                }

            every { configUser.userpagingsize } returns 2
            every { configUser.userAttributesDelta() } returns arrayOf("id", "displayName")

            every {
                graphServiceClient.users().delta().get(any())
            } returns page1

            every {
                graphServiceClient
                    .users()
                    .delta()
                    .withUrl("page-2")
                    .get()
            } returns page2

            every {
                graphServiceClient
                    .users()
                    .delta()
                    .withUrl("page-3")
                    .get()
            } returns page3

            coEvery {
                entraUserSyncService.processPage(any(), any(), false)
            } returnsMany
                listOf(
                    EntraUserSyncService.UserSyncPageResult(publishedUsers = 2, removedUsers = 3),
                    EntraUserSyncService.UserSyncPageResult(publishedUsers = 5, removedUsers = 4),
                    EntraUserSyncService.UserSyncPageResult(publishedUsers = 3, removedUsers = 5),
                )

            coEvery {
                deltaLinkStore.createOrUpdate("users", "delta-link")
            } just Runs

            service.startFullImport(republishAll = false)

            coVerify(exactly = 3) {
                entraUserSyncService.processPage(any(), any(), false)
            }

            coVerify(exactly = 1) {
                entraUserSyncService.processPage(page1.value, any(), false)
            }

            coVerify(exactly = 1) {
                entraUserSyncService.processPage(page2.value, any(), false)
            }

            coVerify(exactly = 1) {
                entraUserSyncService.processPage(page3.value, any(), false)
            }
            // Set log level back to original
            logger.level = oldLevel
        }

    private fun createMsGraphUser() =
        MsGraphUser(
            configUser = configUser,
            graphServiceClient = graphServiceClient,
            entraUserSyncService = entraUserSyncService,
            deltaLinkStore = deltaLinkStore,
            userRepository = userRepository,
            userExternalRepository = userExternalRepository,
        )

    private fun mockDeltaPage(deltaLink: String): DeltaGetResponse =
        mockk(relaxed = true) {
            every { value } returns emptyList()
            every { odataNextLink } returns null
            every { odataDeltaLink } returns deltaLink
        }

    private fun getPrivateField(
        target: Any,
        fieldName: String,
    ): Any? {
        val field = target.javaClass.getDeclaredField(fieldName)
        field.isAccessible = true
        return field.get(target)
    }

    private fun setPrivateField(
        target: Any,
        fieldName: String,
        value: Any?,
    ) {
        val field = target.javaClass.getDeclaredField(fieldName)
        field.isAccessible = true
        field.set(target, value)
    }

    @BeforeEach
    fun beforeEach() {
        configUser = mockk()
        deltaLinkStore = mockk(relaxed = true)
        graphServiceClient = mockk(relaxed = true)
        usersRb = mockk(relaxed = true)
        deltaRb = mockk(relaxed = true)
        countRb = mockk(relaxed = true)
        entraUserSyncService = mockk(relaxed = true)
        userRepository = mockk(relaxed = true)
        userExternalRepository = mockk(relaxed = true)
        service =
            MsGraphUser(
                configUser = configUser,
                graphServiceClient = graphServiceClient,
                entraUserSyncService = entraUserSyncService,
                deltaLinkStore = deltaLinkStore,
                userRepository = userRepository,
                userExternalRepository = userExternalRepository,
            )

        every { configUser.userpagingsize } returns 50
        every { configUser.staleAfterDays } returns 30
        every { configUser.userAttributesDelta() } returns arrayOf("id")
        every { graphServiceClient.users() } returns usersRb
        every { usersRb.delta() } returns deltaRb
        every { usersRb.count() } returns countRb

        coEvery { entraUserSyncService.processPage(any(), any(), any()) } returns
            EntraUserSyncService.UserSyncPageResult(0, 0)
        coEvery { entraUserSyncService.finishFullImport(any()) } returns 0
        coEvery { entraUserSyncService.finishFullImportExternal(any()) } returns 0

        every { userRepository.getCount() } returns 0
        every { userRepository.findStaleObjectIds(any()) } returns emptyList()
        every { userExternalRepository.findStaleObjectIds(any()) } returns emptyList()
        every { userRepository.incrementNotSeenCount(any()) } returns Unit
        every { userExternalRepository.incrementNotSeenCount(any()) } returns Unit
    }

    @AfterEach
    fun tearDown() {
        clearAllMocks()
    }
}
