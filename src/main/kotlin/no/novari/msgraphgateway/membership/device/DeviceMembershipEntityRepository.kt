package no.novari.msgraphgateway.membership.device

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowMapper
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.time.OffsetDateTime

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
                SELECT d.device_ref, d.group_ref, d.status, d.created_at, d.last_updated_at
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
            INSERT INTO device_memberships (device_ref, group_ref, status, created_at, last_updated_at)
            VALUES (?::uuid, ?::uuid, ?, ?, ?)
            ON CONFLICT (device_ref, group_ref)
            DO UPDATE SET
                status = EXCLUDED.status,
                last_updated_at = EXCLUDED.last_updated_at
            """.trimIndent(),
            memberships,
            memberships.size,
        ) { ps, membership ->
            ps.setObject(1, membership.id.deviceRef)
            ps.setObject(2, membership.id.groupRef)
            ps.setString(3, membership.status.name)
            ps.setObject(4, membership.createdAt)
            ps.setObject(5, membership.lastUpdatedAt)
        }
    }

    @Transactional
    fun deleteAll(): Int = jdbcTemplate.update("DELETE FROM device_memberships")

    @Transactional
    fun deleteLastUpdatedBefore(cutoff: OffsetDateTime): Int =
        jdbcTemplate.update(
            """
            DELETE FROM device_memberships
            WHERE last_updated_at < ?
            """.trimIndent(),
            cutoff,
        )

    companion object {
        private val rowMapper =
            RowMapper { rs, _ ->
                DeviceMembershipEntity(
                    id =
                        DeviceMembershipId(
                            rs.getObject("device_ref", java.util.UUID::class.java),
                            rs.getObject("group_ref", java.util.UUID::class.java),
                        ),
                    status = EntraStatus.valueOf(rs.getString("status")),
                    createdAt = rs.getObject("created_at", java.time.OffsetDateTime::class.java),
                    lastUpdatedAt = rs.getObject("last_updated_at", java.time.OffsetDateTime::class.java),
                )
            }
    }
}
