package no.novari.msgraphgateway.membership.device

import jakarta.persistence.Column
import jakarta.persistence.Embeddable
import jakarta.persistence.EmbeddedId
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.Table
import java.io.Serializable
import java.time.OffsetDateTime
import java.util.UUID

@Embeddable
data class DeviceMembershipId(
    @Column(name = "device_ref", nullable = false)
    var deviceRef: UUID = UUID.randomUUID(),
    @Column(name = "resource_ref", nullable = false)
    var resourceRef: UUID = UUID.randomUUID(),
) : Serializable

@Entity
@Table(name = "device_memberships")
class DeviceMembershipEntity(
    @EmbeddedId
    val id: DeviceMembershipId,
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 50)
    val status: EntraStatus,
    @Column(name = "created_at", nullable = false)
    val createdAt: OffsetDateTime,
    @Column(name = "last_updated_at", nullable = false)
    val lastUpdatedAt: OffsetDateTime,
)
