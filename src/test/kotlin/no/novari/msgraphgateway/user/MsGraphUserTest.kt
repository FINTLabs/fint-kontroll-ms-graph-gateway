package no.novari.msgraphgateway.user

import com.microsoft.graph.serviceclient.GraphServiceClient
import com.microsoft.graph.users.delta.DeltaGetResponse
import com.microsoft.kiota.ApiException
import io.mockk.*
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import no.novari.msgraphgateway.MsGraphUser
import no.novari.msgraphgateway.config.ConfigUser
import no.novari.msgraphgateway.entra.DeltaLinkStore
import no.novari.msgraphgateway.user.CoreUserExternalRepository
import no.novari.msgraphgateway.user.CoreUserRepository
import no.novari.msgraphgateway.user.EntraUserSyncService
import org.junit.jupiter.api.Test
import kotlin.reflect.full.callSuspend
import kotlin.reflect.full.declaredFunctions
import kotlin.reflect.jvm.isAccessible

class MsGraphUserTest {
    @Test
    fun fullImportUsers() {
    }

    @Test
    fun pullAllUsersDelta() {
    }

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
            every { deltaRb.get() } throws invalid

            val sut =
                MsGraphUser(
                    configUser = configUser,
                    graphServiceClient = graphServiceClient,
                    entraUserSyncService = mockk(relaxed = true),
                    deltaLinkStore = deltaLinkStore,
                    coreUserRepository = mockk(relaxed = true),
                    coreUserExternalRepository = mockk(relaxed = true),
                )

            setPrivateField(sut, "userDeltaLink", "BAD_LINK")

            sut.pullAllUsersDelta()

            verify(timeout = 2_000, exactly = 1) { deltaRb.withUrl("BAD_LINK") }
            verify(timeout = 2_000, exactly = 1) { deltaRb.get() }

            verify(exactly = 0) {
                deltaRb.get(any())
            }

            verify(exactly = 0) { deltaLinkStore.createOrUpdate(eq("users"), any()) }
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
            val coreUserRepository = mockk<CoreUserRepository>(relaxed = true)
            val coreUserExternalRepository = mockk<CoreUserExternalRepository>(relaxed = true)

            every { coreUserRepository.getCount() } returns 0

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
            every { firstPage.odataDeltaLink } returns "NEW_DELTA"

            every { deltaRb.get(any()) } returns firstPage

            coEvery { entraUserSyncService.processPage(any(), any()) } returns 0
            coEvery { entraUserSyncService.finishFullImport(any()) } returns 0
            coEvery { entraUserSyncService.finishFullImportExternal(any()) } returns 0

            val sut =
                MsGraphUser(
                    configUser = configUser,
                    graphServiceClient = graphServiceClient,
                    entraUserSyncService = entraUserSyncService,
                    deltaLinkStore = deltaLinkStore,
                    coreUserRepository = coreUserRepository,
                    coreUserExternalRepository = coreUserExternalRepository,
                )

            invokePrivateSuspendStartFullImport(sut)

            coVerify(exactly = 1) { deltaLinkStore.createOrUpdate("users", "NEW_DELTA") }
            verify(exactly = 1) { deltaRb.get(any()) }
        }

    private suspend fun invokePrivateSuspendStartFullImport(sut: Any) {
        val kClass = sut::class
        val fn = kClass.declaredFunctions.first { it.name == "startFullImport" }
        fn.isAccessible = true
        fn.callSuspend(sut)
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
}
