package no.novari.msgraphgateway.controller

import io.mockk.every
import io.mockk.mockk
import no.novari.msgraphgateway.repository.user.UserMembershipEntity
import no.novari.msgraphgateway.repository.user.UserMembershipId
import no.novari.msgraphgateway.services.member.UserMembershipService
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

class UserMembershipControllerTest {
    private lateinit var membershipService: UserMembershipService
    private lateinit var controller: UserMembershipController

    @BeforeEach
    fun setUp() {
        membershipService = mockk()
        controller = UserMembershipController(membershipService)
    }

    @Test
    fun `null memberships response contains memberships and count`() {
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

        val response = controller.getMembershipsWithNullStatus()
        val responseBody = requireNotNull(response.body)
        val responseMembership = responseBody.memberships.single()

        assertEquals(200, response.statusCode.value())
        assertEquals(1, responseBody.count)
        assertEquals(membership.id.userRef, responseMembership.userRef)
        assertEquals(membership.id.groupRef, responseMembership.groupRef)
        assertEquals(null, responseMembership.status)
    }
}
