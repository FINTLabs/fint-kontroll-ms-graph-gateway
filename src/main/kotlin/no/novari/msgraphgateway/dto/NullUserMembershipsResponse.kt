package no.novari.msgraphgateway.dto

import no.novari.msgraphgateway.entra.EntraStatus
import java.time.OffsetDateTime
import java.util.UUID

data class UserMembershipResponse(
    val userRef: UUID,
    val groupRef: UUID,
    val status: EntraStatus?,
    val desiredPresent: Boolean?,
    val observedPresent: Boolean?,
    val createdAt: OffsetDateTime,
    val lastUpdatedAt: OffsetDateTime,
)

data class NullUserMembershipsResponse(
    val count: Int,
    val memberships: List<UserMembershipResponse>,
)
