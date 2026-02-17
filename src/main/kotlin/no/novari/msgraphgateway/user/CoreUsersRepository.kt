@file:Suppress("ktlint:standard:no-wildcard-imports")

package no.novari.msgraphgateway.user

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import java.sql.Connection
import java.time.Instant
import java.time.ZoneOffset
import java.util.*

@Repository
class CoreUserRepository(
    jdbc: NamedParameterJdbcTemplate,
) : CoreUsersRepository(jdbc, table = "users")

@Repository
class CoreUserExternalRepository(
    jdbc: NamedParameterJdbcTemplate,
) : CoreUsersRepository(jdbc, table = "users_external")

open class CoreUsersRepository(
    private val jdbc: NamedParameterJdbcTemplate,
    table: String,
) : UserStateRepository {
    private val findStaleWithNotSeenCountGreaterThanSql =
        """
        SELECT object_id
        FROM $table
        WHERE last_seen_at < :cutoff
          AND not_seen_count > :minNotSeenCount
        """.trimIndent()

    private val findStaleObjectsByIdSql =
        """
        SELECT object_id
        FROM $table
        WHERE last_seen_at < :cutoff
        """.trimIndent()

    private val countAllSql =
        """
        SELECT COUNT(*) FROM $table
        """.trimIndent()

    private val deleteByIdSql =
        """
        DELETE FROM $table
        WHERE object_id = :objectId
        """.trimIndent()

    private val deleteByIdsSql =
        """
        DELETE FROM $table
        WHERE object_id IN (:objectIds)
        RETURNING object_id
        """.trimIndent()

    private val incrementNotSeenCountSql =
        """
        UPDATE $table
        SET not_seen_count = not_seen_count + 1
        WHERE object_id IN (:objectIds)
        """.trimIndent()

    private val batchUpsertReturningChangedSql =
        """
        WITH input AS (
          SELECT *
          FROM unnest(
            :objectIds::uuid[],
            :checksums::bytea[],
            :lastSeenAts::timestamptz[]
          ) AS t(object_id, checksum, last_seen_at)
        ),
        input_with_old AS (
          SELECT
            i.object_id,
            i.checksum,
            i.last_seen_at,
            u.checksum AS old_checksum
          FROM input i
          LEFT JOIN $table u ON u.object_id = i.object_id
        ),
        upsert AS (
          INSERT INTO $table (object_id, checksum, last_seen_at, not_seen_count)
          SELECT object_id, checksum, last_seen_at, 0
          FROM input_with_old
          ON CONFLICT (object_id) DO UPDATE
          SET
            last_seen_at   = EXCLUDED.last_seen_at,
            checksum       = CASE
                              WHEN $table.checksum IS DISTINCT FROM EXCLUDED.checksum
                              THEN EXCLUDED.checksum
                              ELSE $table.checksum
                            END,
            not_seen_count = 0
          RETURNING object_id
        )
        SELECT i.object_id
        FROM input_with_old i
        JOIN upsert u USING (object_id)
        WHERE i.old_checksum IS DISTINCT FROM i.checksum
        """.trimIndent()

    override fun findStaleObjectIds(cutoff: Instant): List<UUID> {
        val params = MapSqlParameterSource().addValue("cutoff", cutoff.atOffset(ZoneOffset.UTC))
        return jdbc.query(findStaleObjectsByIdSql, params) { rs, _ ->
            rs.getObject("object_id", UUID::class.java)
        }
    }

    override fun findStaleObjectIdsWithNotSeenCountGreaterThan(
        cutoff: Instant,
        minNotSeenCount: Int,
    ): List<UUID> {
        val params =
            MapSqlParameterSource()
                .addValue("cutoff", cutoff.atOffset(ZoneOffset.UTC))
                .addValue("minNotSeenCount", minNotSeenCount)

        return jdbc.query(findStaleWithNotSeenCountGreaterThanSql, params) { rs, _ ->
            rs.getObject("object_id", UUID::class.java)
        }
    }

    override fun batchUpsertReturningChanged(rows: List<UserStateRepository.UpsertRow>): Set<UUID> {
        if (rows.isEmpty()) return emptySet()

        val objectIds = rows.map { it.objectId }.toTypedArray()
        val checksums = rows.map { it.checksum }.toTypedArray()
        val lastSeenAts = rows.map { it.lastSeenAt }.toTypedArray()

        return jdbc.jdbcTemplate.execute { conn: Connection ->
            val params =
                MapSqlParameterSource()
                    .addValue("objectIds", conn.createArrayOf("uuid", objectIds))
                    .addValue("checksums", conn.createArrayOf("bytea", checksums))
                    .addValue("lastSeenAts", conn.createArrayOf("timestamptz", lastSeenAts))

            jdbc
                .query(batchUpsertReturningChangedSql, params) { rs, _ ->
                    rs.getObject("object_id", UUID::class.java)
                }.toSet()
        } ?: emptySet()
    }

    override fun deleteById(objectId: UUID) {
        jdbc.update(deleteByIdSql, MapSqlParameterSource().addValue("objectId", objectId))
    }

    override fun deleteByIdsReturningObjectIds(objectIds: Collection<UUID>): List<UUID> {
        if (objectIds.isEmpty()) return emptyList()
        val params = MapSqlParameterSource().addValue("objectIds", objectIds)
        return jdbc.query(deleteByIdsSql, params) { rs, _ ->
            rs.getObject("object_id", UUID::class.java)
        }
    }

    override fun incrementNotSeenCount(objectIds: Collection<UUID>) {
        if (objectIds.isEmpty()) return
        jdbc.update(incrementNotSeenCountSql, MapSqlParameterSource().addValue("objectIds", objectIds))
    }

    override fun getCount(): Int = jdbc.queryForObject(countAllSql, MapSqlParameterSource(), Int::class.java) ?: 0

    private val existsByIdSql =
        """
        SELECT EXISTS (
          SELECT 1
          FROM $table
          WHERE object_id = :objectId
        )
        """.trimIndent()

    override fun existsById(objectId: UUID): Boolean {
        val params = MapSqlParameterSource().addValue("objectId", objectId)
        return jdbc.queryForObject(existsByIdSql, params, Boolean::class.java) ?: false
    }
}
