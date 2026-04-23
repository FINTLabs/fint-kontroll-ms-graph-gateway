package no.novari.msgraphgateway.user

import com.microsoft.graph.serviceclient.GraphServiceClient
import com.microsoft.graph.users.delta.DeltaGetResponse
import com.microsoft.kiota.ApiException
import io.mockk.*
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import no.novari.msgraphgateway.config.ConfigUser
import no.novari.msgraphgateway.entra.DeltaLinkStore
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.reflect.full.callSuspend
import kotlin.reflect.full.declaredFunctions
import kotlin.reflect.jvm.isAccessible

class MsGraphUserTest {
    private lateinit var configUser: ConfigUser
    private lateinit var deltaLinkStore: DeltaLinkStore
    private lateinit var graphServiceClient: GraphServiceClient
    private lateinit var usersRb: com.microsoft.graph.users.UsersRequestBuilder
    private lateinit var deltaRb: com.microsoft.graph.users.delta.DeltaRequestBuilder
    private lateinit var entraUserSyncService: EntraUserSyncService
    private lateinit var userRepository: UserRepository
    private lateinit var userExternalRepository: UserExternalRepository

    @Test
    fun invalidDeltaLinkDoesNotCallFreshDelta() =
        runBlocking {
            val configUser = mockk<ConfigUser>()
            every { configUser.userpagingsize } returns 50
            every { configUser.userAttributesDelta() } returns arrayOf("id")

            val deltaLinkStore = mockk<DeltaLinkStore>(relaxed = true)
            val graphServiceClient = mockk<GraphServiceClient>(relaxed = true)
            val usersRb = mockk<com.microsoft.graph.users.UsersRequestBuilder>(relaxed = true)
            val deltaRb = mockk<com.microsoft.graph.users.delta.DeltaRequestBuilder>(relaxed = true)

            every { graphServiceClient.users() } returns usersRb
            every { usersRb.delta() } returns deltaRb

            val invalid = mockk<ApiException>(relaxed = true)
            every { invalid.responseStatusCode } returns 400
            every { invalid.message } returns "Badly formed token"
            every { deltaRb.withUrl("BAD_LINK") } returns deltaRb

            val firstPage = mockk<DeltaGetResponse>(relaxed = true)
            every { firstPage.value } returns emptyList()
            every { firstPage.odataNextLink } returns null
            every { firstPage.odataDeltaLink } returns "new_deltalink"

            every { deltaRb.get(any()) } throws invalid andThen firstPage

            val sut =
                MsGraphUser(
                    configUser = configUser,
                    graphServiceClient = graphServiceClient,
                    entraUserSyncService = mockk(relaxed = true),
                    deltaLinkStore = deltaLinkStore,
                    userRepository = mockk(relaxed = true),
                    userExternalRepository = mockk(relaxed = true),
                )

            setPrivateField(sut, "userDeltaLink", "BAD_LINK")

            sut.pullAllUsersDelta()

            verify(timeout = 2_000, exactly = 1) { deltaRb.withUrl("BAD_LINK") }
            verify(timeout = 2_000, exactly = 2) { deltaRb.get(any()) }
            verify(timeout = 2_000, exactly = 1) { deltaLinkStore.createOrUpdate("users", "") }
            verify(timeout = 2_000, exactly = 1) { deltaLinkStore.createOrUpdate("users", "new_deltalink") }
        }

    @Test
    fun invalidDeltaLinkResetsAndStoresNewDeltaLink() =
        runBlocking {
            val configUser = mockk<ConfigUser>()
            every { configUser.userpagingsize } returns 50
            every { configUser.userAttributesDelta() } returns arrayOf("id")

            val deltaLinkStore = mockk<DeltaLinkStore>(relaxed = true)
            val graphServiceClient = mockk<GraphServiceClient>(relaxed = true)
            val usersRb = mockk<com.microsoft.graph.users.UsersRequestBuilder>(relaxed = true)
            val deltaRb = mockk<com.microsoft.graph.users.delta.DeltaRequestBuilder>(relaxed = true)

            every { graphServiceClient.users() } returns usersRb
            every { usersRb.delta() } returns deltaRb

            val invalid = mockk<ApiException>(relaxed = true)
            every { invalid.responseStatusCode } returns 400
            every { invalid.message } returns "Badly formed token"
            every { deltaRb.withUrl("old_bad_deltalink") } returns deltaRb
            every { deltaRb.get() } throws invalid

            val firstPage = mockk<DeltaGetResponse>(relaxed = true)
            every { firstPage.value } returns emptyList()
            every { firstPage.odataNextLink } returns null
            every { firstPage.odataDeltaLink } returns "new_deltalink"

            every { deltaRb.get(any()) } returns firstPage

            val sut =
                MsGraphUser(
                    configUser = configUser,
                    graphServiceClient = graphServiceClient,
                    entraUserSyncService = mockk(relaxed = true),
                    deltaLinkStore = deltaLinkStore,
                    userRepository = mockk(relaxed = true),
                    userExternalRepository = mockk(relaxed = true),
                )

            setPrivateField(sut, "userDeltaLink", "old_bad_deltalink")

            sut.pullAllUsersDelta()

            verify(timeout = 2_000) { deltaRb.get(any()) }
            verify(exactly = 1) { deltaRb.withUrl("old_bad_deltalink") }
            verify(exactly = 1) { deltaRb.get(any()) }
            verify(timeout = 2_000, exactly = 1) {
                deltaLinkStore.createOrUpdate("users", "new_deltalink")
            }

            val newValue = getPrivateField(sut, "userDeltaLink")
            assertEquals("new_deltalink", newValue)
        }

    @Test
    fun fullImportStoresNewDeltaLink() =
        runTest {
            val configUser = mockk<ConfigUser>()
            every { configUser.userpagingsize } returns 50
            every { configUser.userAttributesDelta() } returns arrayOf("id", "displayName")
            every { configUser.staleAfterDays } returns 30
            every { configUser.acceptedDeviationPercent } returns null

            val deltaLinkStore = mockk<DeltaLinkStore>(relaxed = true)
            val entraUserSyncService = mockk<EntraUserSyncService>(relaxed = true)
            val userRepository = mockk<UserRepository>(relaxed = true)
            val userExternalRepository = mockk<UserExternalRepository>(relaxed = true)

            every { userRepository.getCount() } returns 0

            val graphServiceClient = mockk<GraphServiceClient>(relaxed = true)
            val usersRb = mockk<com.microsoft.graph.users.UsersRequestBuilder>(relaxed = true)
            val deltaRb = mockk<com.microsoft.graph.users.delta.DeltaRequestBuilder>(relaxed = true)
            val countRb = mockk<com.microsoft.graph.users.count.CountRequestBuilder>(relaxed = true)

            every { graphServiceClient.users() } returns usersRb
            every { usersRb.delta() } returns deltaRb
            every { usersRb.count() } returns countRb
            every { countRb.get(any()) } returns 100

            val firstPage = mockk<DeltaGetResponse>(relaxed = true)
            every { firstPage.value } returns emptyList()
            every { firstPage.odataNextLink } returns null
            every { firstPage.odataDeltaLink } returns "new_deltalink"

            every { deltaRb.get(any()) } returns firstPage

            coEvery { entraUserSyncService.processPage(any(), any()) } returns 0
            coEvery { entraUserSyncService.finishFullImport(any()) } returns 0
            coEvery { entraUserSyncService.finishFullImportExternal(any()) } returns 0

            coEvery { userRepository.findStaleObjectIds(any()) } returns emptyList()
            coEvery { userExternalRepository.findStaleObjectIds(any()) } returns emptyList()
            coEvery { userRepository.incrementNotSeenCount(any()) } returns Unit
            coEvery { userExternalRepository.incrementNotSeenCount(any()) } returns Unit

            val sut =
                MsGraphUser(
                    configUser = configUser,
                    graphServiceClient = graphServiceClient,
                    entraUserSyncService = entraUserSyncService,
                    deltaLinkStore = deltaLinkStore,
                    userRepository = userRepository,
                    userExternalRepository = userExternalRepository,
                )

            invokePrivateSuspendStartFullImport(sut)

            verify(exactly = 1) { deltaRb.get(any()) }
            coVerify(exactly = 1) { deltaLinkStore.createOrUpdate("users", "new_deltalink") }
            coVerify(exactly = 1) { entraUserSyncService.finishFullImport(any()) }
            coVerify(exactly = 1) { entraUserSyncService.finishFullImportExternal(any()) }

            val current = getPrivateField(sut, "userDeltaLink") as String?
            assertEquals("new_deltalink", current)
        }

    @Test
    fun fullImportUsers_doesNotStartSecondRunWhileFirstIsRunning() =
        runTest {
            val started = CompletableDeferred<Unit>()
            val finishFirstRun = CompletableDeferred<Unit>()
            var importCount = 0

            val msGraphUser = createMsGraphUser()

            coEvery { msGraphUser["startFullImport"]() } coAnswers {
                importCount++
                if (importCount == 1) {
                    started.complete(Unit)
                    finishFirstRun.await()
                }
            }

            msGraphUser.fullImportUsers()
            started.await()

            msGraphUser.fullImportUsers()

            assertEquals(1, importCount)

            finishFirstRun.complete(Unit)
        }

    @Test
    fun pullAllUsersDelta_doesNotStartSecondRunWhileFirstIsRunning() =
        runTest {
            val firstRunStarted = CompletableDeferred<Unit>()
            val allowFirstRunToFinish = CompletableDeferred<Unit>()

            val msGraphUser = createMsGraphUser()

            val firstPage = mockk<DeltaGetResponse>(relaxed = true)

            coEvery { deltaRb.get(any()) } coAnswers {
                firstRunStarted.complete(Unit)
                allowFirstRunToFinish.await()
                firstPage
            }

            msGraphUser.pullAllUsersDelta()
            firstRunStarted.await()

            msGraphUser.pullAllUsersDelta()
            advanceUntilIdle()

            coVerify(exactly = 1) { deltaRb.get(any()) }

            allowFirstRunToFinish.complete(Unit)
            advanceUntilIdle()
        }

    private suspend fun invokePrivateSuspendStartFullImport(sut: Any) {
        val kClass = sut::class
        val fn = kClass.declaredFunctions.first { it.name == "startFullImport" }
        fn.isAccessible = true
        fn.callSuspend(sut)
    }

    private fun getPrivateField(
        target: Any,
        fieldName: String,
    ): Any? {
        val f = target.javaClass.getDeclaredField(fieldName)
        f.isAccessible = true
        return f.get(target)
    }

    private fun setPrivateField(
        target: Any,
        fieldName: String,
        value: Any?,
    ) {
        val f = target.javaClass.getDeclaredField(fieldName)
        f.isAccessible = true
        f.set(target, value)
    }

    private fun createMsGraphUser() =
        spyk(
            MsGraphUser(
                configUser = configUser,
                graphServiceClient = graphServiceClient,
                entraUserSyncService = entraUserSyncService,
                deltaLinkStore = deltaLinkStore,
                userRepository = userRepository,
                userExternalRepository = userExternalRepository,
            ),
            recordPrivateCalls = true,
        )

    @BeforeEach
    fun beforeEach() {
        configUser = mockk()
        deltaLinkStore = mockk(relaxed = true)
        graphServiceClient = mockk(relaxed = true)
        usersRb = mockk(relaxed = true)
        deltaRb = mockk(relaxed = true)
        entraUserSyncService = mockk(relaxed = true)
        userRepository = mockk(relaxed = true)
        userExternalRepository = mockk(relaxed = true)

        every { configUser.userpagingsize } returns 50
        every { configUser.userAttributesDelta() } returns arrayOf("id")

        every { graphServiceClient.users() } returns usersRb
        every { usersRb.delta() } returns deltaRb

        coEvery { entraUserSyncService.processPage(any(), any()) } returns 0
        coEvery { entraUserSyncService.finishFullImport(any()) } returns 0
        coEvery { entraUserSyncService.finishFullImportExternal(any()) } returns 0

        coEvery { userRepository.findStaleObjectIds(any()) } returns emptyList()
        coEvery { userExternalRepository.findStaleObjectIds(any()) } returns emptyList()
        coEvery { userRepository.incrementNotSeenCount(any()) } returns Unit
        coEvery { userExternalRepository.incrementNotSeenCount(any()) } returns Unit
    }

    @AfterEach
    fun tearDown() {
        clearAllMocks()
    }
}
