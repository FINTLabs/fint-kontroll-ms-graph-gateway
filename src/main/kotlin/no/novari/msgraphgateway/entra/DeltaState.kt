package no.novari.msgraphgateway.entra

import jakarta.persistence.*
import org.hibernate.annotations.UpdateTimestamp
import java.time.Instant

@Entity
@Table(name = "delta_state")
data class DeltaState(
    @Column(nullable = false, unique = true)
    val name: String,
    @Column(name = "delta_link", nullable = false)
    var deltaLink: String,
    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    var updatedAt: Instant? = null,
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,
)
