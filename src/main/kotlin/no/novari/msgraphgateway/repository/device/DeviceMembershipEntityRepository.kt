package no.novari.msgraphgateway.repository.device

import no.novari.msgraphgateway.entra.EntraStatus
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowMapper
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.time.OffsetDateTime
import java.util.UUID

@Repository
class DeviceMembershipEntityRepository(
    private val jdbcTemplate: JdbcTemplate,
) {
    fun findAllByIds(ids: Collection<DeviceMembershipId>): Map<DeviceMembershipId, DeviceMembershipEntity> {
        if (ids.isEmpty()) {
            return emptyMap()
        }

        val placeholders = ids.joinToString(", ") { "(?::uuid, ?::uuid)" }
        val args =
            ids
                .flatMap { listOf(it.deviceRef, it.groupRef) }
                .toTypedArray()

        return jdbcTemplate
            .query(
                """
                SELECT d.device_ref, d.group_ref, d.status, d.desired_present, d.observed_present,
                       d.created_at, d.last_updated_at
                FROM device_memberships d
                JOIN (VALUES $placeholders) AS v(device_ref, group_ref)
                    ON d.device_ref = v.device_ref
                    AND d.group_ref = v.group_ref
                """.trimIndent(),
                rowMapper,
                *args,
            ).associateBy { it.id }
    }

    @Transactional
    fun saveAll(memberships: Collection<DeviceMembershipEntity>) {
        if (memberships.isEmpty()) {
            return
        }

        jdbcTemplate.batchUpdate(
            """
            INSERT INTO device_memberships
                (device_ref, group_ref, status, desired_present, observed_present, created_at, last_updated_at)
            VALUES (?::uuid, ?::uuid, ?, ?, ?, ?, ?)
            ON CONFLICT (device_ref, group_ref)
            DO UPDATE SET
                status = EXCLUDED.status,
                desired_present = EXCLUDED.desired_present,
                last_updated_at = EXCLUDED.last_updated_at
            """.trimIndent(),
            memberships,
            memberships.size,
        ) { ps, membership ->
            ps.setObject(1, membership.id.deviceRef)
            ps.setObject(2, membership.id.groupRef)
            ps.setString(3, membership.status?.name)
            ps.setObject(4, membership.desiredPresent)
            ps.setObject(5, membership.observedPresent)
            ps.setObject(6, membership.createdAt)
            ps.setObject(7, membership.lastUpdatedAt)
        }
    }

    @Transactional
    fun deleteAll(): Int {
        val deleted = jdbcTemplate.update("DELETE FROM device_memberships WHERE observed_present IS DISTINCT FROM TRUE")
        val cleared =
            jdbcTemplate.update(
                "UPDATE device_memberships SET status = NULL, desired_present = NULL " +
                    "WHERE observed_present IS TRUE AND (status IS NOT NULL OR desired_present IS NOT NULL)",
            )
        return deleted + cleared
    }

    @Transactional
    fun deleteLastUpdatedBefore(cutoff: OffsetDateTime): Int {
        val deleted =
            jdbcTemplate.update(
                """
                DELETE FROM device_memberships
                WHERE last_updated_at < ?
                    AND observed_present IS DISTINCT FROM TRUE
                    AND (status IS NOT NULL OR desired_present IS NOT NULL)
                """.trimIndent(),
                cutoff,
            )
        val cleared =
            jdbcTemplate.update(
                """
                UPDATE device_memberships
                SET status = NULL, desired_present = NULL
                WHERE last_updated_at < ?
                    AND observed_present IS TRUE
                    AND (status IS NOT NULL OR desired_present IS NOT NULL)
                """.trimIndent(),
                cutoff,
            )
        return deleted + cleared
    }

    companion object {
        private val rowMapper =
            RowMapper { rs, _ ->
                DeviceMembershipEntity(
                    id =
                        DeviceMembershipId(
                            rs.getObject("device_ref", UUID::class.java),
                            rs.getObject("group_ref", UUID::class.java),
                        ),
                    status = rs.getString("status")?.let(EntraStatus::valueOf),
                    desiredPresent = rs.getObject("desired_present") as? Boolean,
                    observedPresent = rs.getObject("observed_present") as? Boolean,
                    createdAt = rs.getObject("created_at", OffsetDateTime::class.java),
                    lastUpdatedAt = rs.getObject("last_updated_at", OffsetDateTime::class.java),
                )
            }
    }
}
