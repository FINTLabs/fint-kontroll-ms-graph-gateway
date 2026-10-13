package no.novari.msgraphgateway.controller

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.novari.msgraphgateway.repository.user.UserMembershipEntity
import no.novari.msgraphgateway.repository.user.UserMembershipId
import no.novari.msgraphgateway.services.member.UserMembershipService
import org.hamcrest.Matchers.nullValue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import java.time.OffsetDateTime
import java.util.UUID

class UserMembershipControllerTest {
    private lateinit var membershipService: UserMembershipService
    private lateinit var mockMvc: MockMvc

    @BeforeEach
    fun setUp() {
        membershipService = mockk()
        mockMvc = MockMvcBuilders.standaloneSetup(UserMembershipController(membershipService)).build()
    }

    @Test
    fun `GET without-status returns memberships and count`() {
        val membership =
            UserMembershipEntity(
                id = UserMembershipId(UUID.randomUUID(), UUID.randomUUID()),
                status = null,
                desiredPresent = null,
                observedPresent = true,
                createdAt = OffsetDateTime.now().minusDays(1),
                lastUpdatedAt = OffsetDateTime.now(),
            )
        every { membershipService.findMembershipsWithNullStatus() } returns listOf(membership)

        mockMvc
            .perform(get("/api/admin/user-memberships/without-status"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.count").value(1))
            .andExpect(jsonPath("$.memberships.length()").value(1))
            .andExpect(jsonPath("$.memberships[0].userRef").value(membership.id.userRef.toString()))
            .andExpect(jsonPath("$.memberships[0].groupRef").value(membership.id.groupRef.toString()))
            .andExpect(jsonPath("$.memberships[0].status").value(nullValue()))
            .andExpect(jsonPath("$.memberships[0].desiredPresent").value(nullValue()))
            .andExpect(jsonPath("$.memberships[0].observedPresent").value(true))

        verify(exactly = 1) { membershipService.findMembershipsWithNullStatus() }
    }
}
