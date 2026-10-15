package no.novari.msgraphgateway.controller

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.novari.msgraphgateway.entra.EntraStatus
import no.novari.msgraphgateway.repository.user.UserMembershipEntity
import no.novari.msgraphgateway.repository.user.UserMembershipId
import no.novari.msgraphgateway.services.member.UserMembershipService
import org.hamcrest.Matchers.nullValue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.content
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
    fun `GET returns a page of observed memberships without desired state`() {
        val membership =
            UserMembershipEntity(
                id = UserMembershipId(UUID.randomUUID(), UUID.randomUUID()),
                status = EntraStatus.ADDED,
                desiredPresent = null,
                observedPresent = true,
                createdAt = OffsetDateTime.now().minusDays(1),
                lastUpdatedAt = OffsetDateTime.now(),
            )
        val nextMembership =
            UserMembershipEntity(
                id = UserMembershipId(UUID.randomUUID(), UUID.randomUUID()),
                status = EntraStatus.ADDED,
                desiredPresent = null,
                observedPresent = true,
                createdAt = OffsetDateTime.now().minusDays(1),
                lastUpdatedAt = OffsetDateTime.now(),
            )
        every {
            membershipService.findObservedMembershipsWithoutDesiredState(limit = 2, offset = 2)
        } returns listOf(membership, nextMembership)

        mockMvc
            .perform(
                get("/api/admin/user-memberships/without-desired-state")
                    .param("page", "2")
                    .param("size", "1"),
            ).andExpect(status().isOk)
            .andExpect(jsonPath("$.count").value(1))
            .andExpect(jsonPath("$.page").value(2))
            .andExpect(jsonPath("$.size").value(1))
            .andExpect(jsonPath("$.hasNextPage").value(true))
            .andExpect(jsonPath("$.memberships.length()").value(1))
            .andExpect(jsonPath("$.memberships[0].userRef").value(membership.id.userRef.toString()))
            .andExpect(jsonPath("$.memberships[0].groupRef").value(membership.id.groupRef.toString()))
            .andExpect(jsonPath("$.memberships[0].status").value("ADDED"))
            .andExpect(jsonPath("$.memberships[0].desiredPresent").value(nullValue()))
            .andExpect(jsonPath("$.memberships[0].observedPresent").value(true))

        verify(exactly = 1) {
            membershipService.findObservedMembershipsWithoutDesiredState(limit = 2, offset = 2)
        }
    }

    @Test
    fun `DELETE on delete-all-memberships returns ok`() {
        val deletedCount = 10

        every {
            membershipService.deleteAllMemberships()
        } returns deletedCount

        mockMvc
            .perform(
                delete("/api/admin/user-memberships/delete-all-memberships"),
            ).andDo { println(it.response.contentAsString) }
            .andExpect(status().isOk)
            .andExpect(content().string("Deleted all user memberships, total: $deletedCount"))

        verify(exactly = 1) {
            membershipService.deleteAllMemberships()
        }
    }

    @Test
    fun `DELETE returns OK when before parameter is provided`() {
        val before = OffsetDateTime.now().minusDays(30)
        val deletedCount = 10

        every {
            membershipService.deleteMembershipsUpdatedBefore(before)
        } returns deletedCount

        mockMvc
            .perform(
                delete("/api/admin/user-memberships/delete-all-memberships/before")
                    .param("before", before.toString()),
            ).andDo { println(it.response.contentAsString) }
            .andExpect(status().isOk)
            .andExpect(content().string("Deleted all user memberships before $before, total: $deletedCount"))

        verify(exactly = 1) {
            membershipService.deleteMembershipsUpdatedBefore(before)
        }
    }

    @Test
    fun `DELETE returns Bad Request when before parameter is missing`() {
        mockMvc
            .perform(
                delete("/api/admin/user-memberships/delete-all-memberships/before"),
            ).andExpect(status().isBadRequest)

        verify(exactly = 0) {
            membershipService.deleteMembershipsUpdatedBefore(any())
        }
    }
}
