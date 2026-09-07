package no.novari.msgraphgateway.repository.membership

import no.novari.msgraphgateway.repository.device.DeviceMembershipId
import no.novari.msgraphgateway.repository.user.UserMembershipId
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.util.UUID

@Repository
class GroupMembershipStateRepository(
    private val jdbcTemplate: JdbcTemplate,
) {
    fun markUsersSeen(
        runId: UUID,
        memberships: Collection<UserMembershipId>,
    ) {
        if (memberships.isEmpty()) return
        jdbcTemplate.batchUpdate(
            """
            INSERT INTO user_memberships
                (user_ref, group_ref, status, desired_present, observed_present, observed_run_id,
                 last_observed_at, created_at, last_updated_at)
            VALUES (?::uuid, ?::uuid, NULL, NULL, NULL, ?::uuid, now(), now(), now())
            ON CONFLICT (user_ref, group_ref)
            DO UPDATE SET
                observed_run_id = EXCLUDED.observed_run_id,
                last_observed_at = EXCLUDED.last_observed_at
            """.trimIndent(),
            memberships,
            minOf(DB_BATCH_SIZE, memberships.size),
        ) { statement, membership ->
            statement.setObject(1, membership.userRef)
            statement.setObject(2, membership.groupRef)
            statement.setObject(3, runId)
        }
    }

    fun markDevicesSeen(
        runId: UUID,
        memberships: Collection<DeviceMembershipId>,
    ) {
        if (memberships.isEmpty()) return
        jdbcTemplate.batchUpdate(
            """
            INSERT INTO device_memberships
                (device_ref, group_ref, status, desired_present, observed_present, observed_run_id,
                 last_observed_at, created_at, last_updated_at)
            VALUES (?::uuid, ?::uuid, NULL, NULL, NULL, ?::uuid, now(), now(), now())
            ON CONFLICT (device_ref, group_ref)
            DO UPDATE SET
                observed_run_id = EXCLUDED.observed_run_id,
                last_observed_at = EXCLUDED.last_observed_at
            """.trimIndent(),
            memberships,
            minOf(DB_BATCH_SIZE, memberships.size),
        ) { statement, membership ->
            statement.setObject(1, membership.deviceRef)
            statement.setObject(2, membership.groupRef)
            statement.setObject(3, runId)
        }
    }

    fun unmarkUsersSeen(
        runId: UUID,
        memberships: Collection<UserMembershipId>,
    ) {
        if (memberships.isEmpty()) return
        jdbcTemplate.batchUpdate(
            """
            UPDATE user_memberships
            SET observed_run_id = NULL
            WHERE user_ref = ?::uuid
                AND group_ref = ?::uuid
                AND observed_run_id = ?::uuid
            """.trimIndent(),
            memberships,
            minOf(DB_BATCH_SIZE, memberships.size),
        ) { statement, membership ->
            statement.setObject(1, membership.userRef)
            statement.setObject(2, membership.groupRef)
            statement.setObject(3, runId)
        }
    }

    fun unmarkDevicesSeen(
        runId: UUID,
        memberships: Collection<DeviceMembershipId>,
    ) {
        if (memberships.isEmpty()) return
        jdbcTemplate.batchUpdate(
            """
            UPDATE device_memberships
            SET observed_run_id = NULL
            WHERE device_ref = ?::uuid
                AND group_ref = ?::uuid
                AND observed_run_id = ?::uuid
            """.trimIndent(),
            memberships,
            minOf(DB_BATCH_SIZE, memberships.size),
        ) { statement, membership ->
            statement.setObject(1, membership.deviceRef)
            statement.setObject(2, membership.groupRef)
            statement.setObject(3, runId)
        }
    }

    fun unmarkGroupSeen(
        runId: UUID,
        groupId: UUID,
    ) {
        jdbcTemplate.update(
            "UPDATE user_memberships SET observed_run_id = NULL WHERE group_ref = ?::uuid AND observed_run_id = ?::uuid",
            groupId,
            runId,
        )
        jdbcTemplate.update(
            "UPDATE device_memberships SET observed_run_id = NULL WHERE group_ref = ?::uuid AND observed_run_id = ?::uuid",
            groupId,
            runId,
        )
    }

    fun addObservedUsers(memberships: Collection<UserMembershipId>) {
        updateObservedUsers(memberships, true)
    }

    @Transactional
    fun removeObservedUsers(memberships: Collection<UserMembershipId>) {
        updateObservedUsers(memberships, false)
        if (memberships.isEmpty()) return
        jdbcTemplate.batchUpdate(
            "DELETE FROM user_memberships WHERE user_ref = ?::uuid AND group_ref = ?::uuid " +
                "AND observed_present IS FALSE AND desired_present IS FALSE",
            memberships,
            minOf(DB_BATCH_SIZE, memberships.size),
        ) { statement, membership ->
            statement.setObject(1, membership.userRef)
            statement.setObject(2, membership.groupRef)
        }
    }

    fun addObservedDevices(memberships: Collection<DeviceMembershipId>) {
        updateObservedDevices(memberships, true)
    }

    fun removeObservedDevices(memberships: Collection<DeviceMembershipId>) {
        updateObservedDevices(memberships, false)
    }

    fun findObservedUsersByGroup(groupId: UUID): List<UserMembershipId> =
        jdbcTemplate.query(
            "SELECT user_ref, group_ref FROM user_memberships WHERE group_ref = ?::uuid AND observed_present IS TRUE",
            { resultSet, _ ->
                UserMembershipId(
                    resultSet.getObject("user_ref", UUID::class.java),
                    resultSet.getObject("group_ref", UUID::class.java),
                )
            },
            groupId,
        )

    fun findObservedDevicesByGroup(groupId: UUID): List<DeviceMembershipId> =
        jdbcTemplate.query(
            "SELECT device_ref, group_ref FROM device_memberships WHERE group_ref = ?::uuid AND observed_present IS TRUE",
            { resultSet, _ ->
                DeviceMembershipId(
                    resultSet.getObject("device_ref", UUID::class.java),
                    resultSet.getObject("group_ref", UUID::class.java),
                )
            },
            groupId,
        )

    fun forEachSnapshotUserAddition(
        runId: UUID,
        republishAll: Boolean,
        consumer: (UserMembershipId) -> Unit,
    ) {
        val changedFilter = if (republishAll) "" else "AND observed_present IS DISTINCT FROM TRUE"
        forEachUser(
            """
            SELECT user_ref, group_ref
            FROM user_memberships
            WHERE observed_run_id = ?::uuid
                $changedFilter
                AND (?::uuid IS NULL OR (user_ref, group_ref) > (?::uuid, ?::uuid))
            ORDER BY user_ref, group_ref
            LIMIT $PAGE_SIZE
            """.trimIndent(),
            runId,
            consumer,
        )
    }

    fun forEachSnapshotDeviceAddition(
        runId: UUID,
        republishAll: Boolean,
        consumer: (DeviceMembershipId) -> Unit,
    ) {
        val changedFilter = if (republishAll) "" else "AND observed_present IS DISTINCT FROM TRUE"
        forEachDevice(
            """
            SELECT device_ref, group_ref
            FROM device_memberships
            WHERE observed_run_id = ?::uuid
                $changedFilter
                AND (?::uuid IS NULL OR (device_ref, group_ref) > (?::uuid, ?::uuid))
            ORDER BY device_ref, group_ref
            LIMIT $PAGE_SIZE
            """.trimIndent(),
            runId,
            consumer,
        )
    }

    fun forEachMissingUser(
        runId: UUID,
        consumer: (UserMembershipId) -> Unit,
    ) {
        forEachUser(
            """
            SELECT membership.user_ref, membership.group_ref
            FROM user_memberships membership
            JOIN groups owned_group ON owned_group.object_id = membership.group_ref
            WHERE membership.observed_run_id IS DISTINCT FROM ?::uuid
                AND (membership.observed_present IS TRUE OR membership.desired_present IS TRUE)
                AND (?::uuid IS NULL OR (membership.user_ref, membership.group_ref) > (?::uuid, ?::uuid))
            ORDER BY membership.user_ref, membership.group_ref
            LIMIT $PAGE_SIZE
            """.trimIndent(),
            runId,
            consumer,
        )
    }

    fun forEachMissingDevice(
        runId: UUID,
        consumer: (DeviceMembershipId) -> Unit,
    ) {
        forEachDevice(
            """
            SELECT membership.device_ref, membership.group_ref
            FROM device_memberships membership
            JOIN groups owned_group ON owned_group.object_id = membership.group_ref
            WHERE membership.observed_run_id IS DISTINCT FROM ?::uuid
                AND (membership.observed_present IS TRUE OR membership.desired_present IS TRUE)
                AND (?::uuid IS NULL OR (membership.device_ref, membership.group_ref) > (?::uuid, ?::uuid))
            ORDER BY membership.device_ref, membership.group_ref
            LIMIT $PAGE_SIZE
            """.trimIndent(),
            runId,
            consumer,
        )
    }

    @Transactional
    fun completeSnapshot(runId: UUID) {
        jdbcTemplate.update(
            "UPDATE user_memberships SET observed_present = TRUE " +
                "WHERE observed_run_id = ?::uuid AND observed_present IS DISTINCT FROM TRUE",
            runId,
        )
        jdbcTemplate.update(
            "UPDATE user_memberships SET observed_present = FALSE, last_observed_at = now() " +
                "WHERE observed_run_id IS DISTINCT FROM ?::uuid " +
                "AND EXISTS (SELECT 1 FROM groups WHERE object_id = user_memberships.group_ref)",
            runId,
        )
        jdbcTemplate.update(
            "UPDATE device_memberships SET observed_present = TRUE " +
                "WHERE observed_run_id = ?::uuid AND observed_present IS DISTINCT FROM TRUE",
            runId,
        )
        jdbcTemplate.update(
            "UPDATE device_memberships SET observed_present = FALSE " +
                "WHERE observed_present IS TRUE AND observed_run_id IS DISTINCT FROM ?::uuid",
            runId,
        )
        deleteUnneededState()
    }

    @Transactional
    fun discardSnapshot(runId: UUID) {
        jdbcTemplate.update(
            "DELETE FROM user_memberships WHERE observed_run_id = ?::uuid " +
                "AND observed_present IS NULL AND desired_present IS NULL AND status IS NULL",
            runId,
        )
        jdbcTemplate.update(
            "DELETE FROM device_memberships WHERE observed_run_id = ?::uuid " +
                "AND observed_present IS NULL AND desired_present IS NULL AND status IS NULL",
            runId,
        )
    }

    private fun updateObservedUsers(
        memberships: Collection<UserMembershipId>,
        present: Boolean,
    ) {
        if (memberships.isEmpty()) return
        if (present) {
            jdbcTemplate.batchUpdate(
                """
                INSERT INTO user_memberships
                    (user_ref, group_ref, status, desired_present, observed_present, observed_run_id,
                     last_observed_at, created_at, last_updated_at)
                VALUES (?::uuid, ?::uuid, NULL, NULL, TRUE, NULL, now(), now(), now())
                ON CONFLICT (user_ref, group_ref)
                DO UPDATE SET observed_present = TRUE, observed_run_id = NULL, last_observed_at = now()
                """.trimIndent(),
                memberships,
                minOf(DB_BATCH_SIZE, memberships.size),
            ) { statement, membership ->
                statement.setObject(1, membership.userRef)
                statement.setObject(2, membership.groupRef)
            }
        } else {
            jdbcTemplate.batchUpdate(
                """
                UPDATE user_memberships
                SET observed_present = FALSE, observed_run_id = NULL, last_observed_at = now()
                WHERE user_ref = ?::uuid AND group_ref = ?::uuid
                """.trimIndent(),
                memberships,
                minOf(DB_BATCH_SIZE, memberships.size),
            ) { statement, membership ->
                statement.setObject(1, membership.userRef)
                statement.setObject(2, membership.groupRef)
            }
        }
    }

    private fun updateObservedDevices(
        memberships: Collection<DeviceMembershipId>,
        present: Boolean,
    ) {
        if (memberships.isEmpty()) return
        if (present) {
            jdbcTemplate.batchUpdate(
                """
                INSERT INTO device_memberships
                    (device_ref, group_ref, status, desired_present, observed_present, observed_run_id,
                     last_observed_at, created_at, last_updated_at)
                VALUES (?::uuid, ?::uuid, NULL, NULL, TRUE, NULL, now(), now(), now())
                ON CONFLICT (device_ref, group_ref)
                DO UPDATE SET observed_present = TRUE, observed_run_id = NULL, last_observed_at = now()
                """.trimIndent(),
                memberships,
                minOf(DB_BATCH_SIZE, memberships.size),
            ) { statement, membership ->
                statement.setObject(1, membership.deviceRef)
                statement.setObject(2, membership.groupRef)
            }
        } else {
            jdbcTemplate.batchUpdate(
                """
                UPDATE device_memberships
                SET observed_present = FALSE, observed_run_id = NULL, last_observed_at = now()
                WHERE device_ref = ?::uuid AND group_ref = ?::uuid
                """.trimIndent(),
                memberships,
                minOf(DB_BATCH_SIZE, memberships.size),
            ) { statement, membership ->
                statement.setObject(1, membership.deviceRef)
                statement.setObject(2, membership.groupRef)
            }
        }
    }

    private fun forEachUser(
        sql: String,
        runId: UUID,
        consumer: (UserMembershipId) -> Unit,
    ) {
        var after: UserMembershipId? = null
        do {
            val page =
                jdbcTemplate.query(
                    sql,
                    { resultSet, _ ->
                        UserMembershipId(
                            resultSet.getObject("user_ref", UUID::class.java),
                            resultSet.getObject("group_ref", UUID::class.java),
                        )
                    },
                    runId,
                    after?.userRef,
                    after?.userRef,
                    after?.groupRef,
                )
            page.forEach(consumer)
            after = page.lastOrNull()
        } while (page.size == PAGE_SIZE)
    }

    private fun forEachDevice(
        sql: String,
        runId: UUID,
        consumer: (DeviceMembershipId) -> Unit,
    ) {
        var after: DeviceMembershipId? = null
        do {
            val page =
                jdbcTemplate.query(
                    sql,
                    { resultSet, _ ->
                        DeviceMembershipId(
                            resultSet.getObject("device_ref", UUID::class.java),
                            resultSet.getObject("group_ref", UUID::class.java),
                        )
                    },
                    runId,
                    after?.deviceRef,
                    after?.deviceRef,
                    after?.groupRef,
                )
            page.forEach(consumer)
            after = page.lastOrNull()
        } while (page.size == PAGE_SIZE)
    }

    private fun deleteUnneededState() {
        jdbcTemplate.update(
            "DELETE FROM user_memberships WHERE observed_present IS FALSE " +
                "AND (desired_present IS FALSE OR (desired_present IS NULL AND status IS NULL))",
        )
        jdbcTemplate.update(
            "DELETE FROM device_memberships WHERE observed_present IS FALSE AND desired_present IS NULL AND status IS NULL",
        )
    }

    companion object {
        private const val DB_BATCH_SIZE = 5_000
        private const val PAGE_SIZE = 5_000
    }
}
