package no.novari.msgraphgateway.controller

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.novari.msgraphgateway.user.MsGraphUser
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class UserControllerTest {
    private lateinit var groupService: GroupService
    private lateinit var msGraphUser: MsGraphUser
    private lateinit var userService: UserService
    private lateinit var controller: UserController

    @Test
    fun getUserWithGroupsReturnsNotFoundWhenUserIsMissing() {
        every { groupService.getEntraUserWithGroups("missing") } returns UserWithGroupsDto(user = null)

        val response = controller.getUserWithGroups("missing")

        assertEquals(404, response.statusCode.value())
    }

    @Test
    fun triggerUserFullSyncStartsNormalFullImport() {
        val response = controller.triggerUserFullSync()

        verify(exactly = 1) { userService.triggerFullImport(false) }
        assertEquals(202, response.statusCode.value())
    }

    @Test
    fun triggerUserDeltaSyncStartsDeltaImport() {
        val response = controller.triggerUserDeltaSync()

        verify(exactly = 1) { msGraphUser.pullAllUsersDelta() }
        assertEquals(202, response.statusCode.value())
    }

    @Test
    fun triggerRepublishAndFullSyncStartsRepublishImport() {
        val response = controller.triggerRepublishAndFullSync()

        verify(exactly = 1) { userService.triggerFullImport(true) }
        assertEquals(202, response.statusCode.value())
    }

    @BeforeEach
    fun setUp() {
        groupService = mockk()
        msGraphUser = mockk(relaxed = true)
        userService = mockk(relaxed = true)
        controller = UserController(groupService, msGraphUser, userService)
    }
}
