package no.novari.msgraphgateway.repository.user

import no.novari.msgraphgateway.entra.EntraStatus
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowMapper
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.time.OffsetDateTime
import java.util.UUID

@Repository
class UserMembershipEntityRepository(
    private val jdbcTemplate: JdbcTemplate,
) {
    fun findAllByGroupRef(groupRef: UUID): Map<UserMembershipId, UserMembershipEntity> =
        jdbcTemplate
            .query(
                """
                SELECT user_ref, group_ref, status, created_at, last_updated_at
                FROM user_memberships
                WHERE group_ref = ?::uuid
                """.trimIndent(),
                rowMapper,
                groupRef,
            ).associateBy { it.id }

    fun findAllByIds(ids: Collection<UserMembershipId>): Map<UserMembershipId, UserMembershipEntity> {
        if (ids.isEmpty()) {
            return emptyMap()
        }

        val placeholders = ids.joinToString(", ") { "(?::uuid, ?::uuid)" }
        val args =
            ids
                .flatMap { listOf(it.userRef, it.groupRef) }
                .toTypedArray()

        return jdbcTemplate
            .query(
                """
                SELECT d.user_ref, d.group_ref, d.status, d.created_at, d.last_updated_at
                FROM user_memberships d
                JOIN (VALUES $placeholders) AS v(user_ref, group_ref)
                    ON d.user_ref = v.user_ref
                    AND d.group_ref = v.group_ref
                """.trimIndent(),
                rowMapper,
                *args,
            ).associateBy { it.id }
    }

    @Transactional
    fun saveAll(memberships: Collection<UserMembershipEntity>) {
        if (memberships.isEmpty()) {
            return
        }

        jdbcTemplate.batchUpdate(
            """
            INSERT INTO user_memberships (user_ref, group_ref, status, created_at, last_updated_at)
            VALUES (?::uuid, ?::uuid, ?, ?, ?)
            ON CONFLICT (user_ref, group_ref)
            DO UPDATE SET
                status = EXCLUDED.status,
                last_updated_at = EXCLUDED.last_updated_at
            """.trimIndent(),
            memberships,
            memberships.size,
        ) { ps, membership ->
            ps.setObject(1, membership.id.userRef)
            ps.setObject(2, membership.id.groupRef)
            ps.setString(3, membership.status.name)
            ps.setObject(4, membership.createdAt)
            ps.setObject(5, membership.lastUpdatedAt)
        }
    }

    @Transactional
    fun replaceGroupMemberships(
        groupRef: UUID,
        userRefs: Collection<UUID>,
        updatedAt: OffsetDateTime,
    ) {
        saveAll(
            userRefs.distinct().map { userRef ->
                UserMembershipEntity(
                    id = UserMembershipId(userRef, groupRef),
                    status = EntraStatus.ADDED,
                    createdAt = updatedAt,
                    lastUpdatedAt = updatedAt,
                )
            },
        )
    }

    @Transactional
    fun deleteAll(): Int = jdbcTemplate.update("DELETE FROM user_memberships")

    @Transactional
    fun deleteLastUpdatedBefore(cutoff: OffsetDateTime): Int =
        jdbcTemplate.update(
            """
            DELETE FROM user_memberships
            WHERE last_updated_at < ?
            """.trimIndent(),
            cutoff,
        )

    companion object {
        private val rowMapper =
            RowMapper { rs, _ ->
                UserMembershipEntity(
                    id =
                        UserMembershipId(
                            rs.getObject("user_ref", UUID::class.java),
                            rs.getObject("group_ref", UUID::class.java),
                        ),
                    status = EntraStatus.valueOf(rs.getString("status")),
                    createdAt = rs.getObject("created_at", OffsetDateTime::class.java),
                    lastUpdatedAt = rs.getObject("last_updated_at", OffsetDateTime::class.java),
                )
            }
    }
}
