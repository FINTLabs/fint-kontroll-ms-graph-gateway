package no.novari.msgraphgateway.repository.user

import jakarta.persistence.Column
import jakarta.persistence.Embeddable
import jakarta.persistence.EmbeddedId
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.Table
import no.novari.msgraphgateway.entra.EntraStatus
import java.io.Serializable
import java.time.OffsetDateTime
import java.util.UUID

@Embeddable
data class UserMembershipId(
    @Column(name = "user_ref", nullable = false)
    var userRef: UUID,
    @Column(name = "group_ref", nullable = false)
    var groupRef: UUID,
) : Serializable

@Entity
@Table(name = "user_memberships")
class UserMembershipEntity(
    @EmbeddedId
    val id: UserMembershipId,
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 50)
    val status: EntraStatus?,
    @Column(name = "desired_present")
    val desiredPresent: Boolean?,
    @Column(name = "observed_present")
    val observedPresent: Boolean?,
    @Column(name = "created_at", nullable = false)
    val createdAt: OffsetDateTime,
    @Column(name = "last_updated_at", nullable = false)
    val lastUpdatedAt: OffsetDateTime,
)
