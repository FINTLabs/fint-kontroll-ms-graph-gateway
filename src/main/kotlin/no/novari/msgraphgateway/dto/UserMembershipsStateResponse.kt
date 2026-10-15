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

data class UserMembershipsStateResponse(
    val count: Int,
    val page: Int,
    val size: Int,
    val hasNextPage: Boolean,
    val memberships: List<UserMembershipResponse>,
)
