package no.novari.msgraphgateway.user

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import java.sql.Connection
import java.sql.ResultSet
import java.time.Instant
import java.time.ZoneOffset
import java.util.*

@Repository
class CoreUserRepository(
    private val jdbc: NamedParameterJdbcTemplate,
) {
    private val findStaleObjectsByIdSql =
        """
        SELECT object_id
        FROM users
        WHERE last_seen_at < :cutoff
        """.trimIndent()

    private val findStaleWithNotSeenCountGreaterThanSql =
        """
        SELECT object_id
        FROM users
        WHERE last_seen_at < :cutoff
          AND not_seen_count > :minNotSeenCount
        """.trimIndent()

    private val countAllSql =
        """
        SELECT COUNT(*) FROM users
        """.trimIndent()

    private val deleteByIdSql =
        """
        DELETE FROM users
        WHERE object_id = :objectId
        """.trimIndent()

    private val deleteByIdsSql =
        """
        DELETE FROM users
        WHERE object_id IN (:objectIds)
        RETURNING object_id
        """.trimIndent()

    /**
     * Upserts (object_id, checksum, last_seen_at).
     * - created_at: set by DEFAULT on insert, never overwritten on update
     * - not_seen_count: reset to 0 on update/insert (since we just "saw" it)
     * Returns object_ids where checksum changed compared to previous value.
     */
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
          LEFT JOIN users u ON u.object_id = i.object_id
        ),
        upsert AS (
          INSERT INTO users (object_id, checksum, last_seen_at, not_seen_count)
          SELECT object_id, checksum, last_seen_at, 0
          FROM input_with_old
          ON CONFLICT (object_id) DO UPDATE
          SET
            last_seen_at   = EXCLUDED.last_seen_at,
            checksum       = CASE
                              WHEN users.checksum IS DISTINCT FROM EXCLUDED.checksum
                              THEN EXCLUDED.checksum
                              ELSE users.checksum
                            END,
            not_seen_count = 0
          RETURNING object_id
        )
        SELECT i.object_id
        FROM input_with_old i
        JOIN upsert u USING (object_id)
        WHERE i.old_checksum IS DISTINCT FROM i.checksum
        """.trimIndent()

    data class UpsertRow(
        val objectId: UUID,
        val checksum: ByteArray,
        val lastSeenAt: Instant,
    )

    fun batchUpsertReturningChanged(rows: List<UpsertRow>): Set<UUID> {
        if (rows.isEmpty()) return emptySet()

        val objectIds = rows.map { it.objectId }.toTypedArray()
        val checksums = rows.map { it.checksum }.toTypedArray()
        val lastSeenAts = rows.map { it.lastSeenAt }.toTypedArray()

        val result =
            jdbc.jdbcTemplate.execute { conn: Connection ->
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

        return result
    }

    fun findStaleObjectIds(cutoff: Instant): List<UUID> {
        val params = MapSqlParameterSource().addValue("cutoff", cutoff.atOffset(ZoneOffset.UTC))
        return jdbc.query(
            findStaleObjectsByIdSql,
            params,
            { rs: ResultSet?, _: Int -> rs!!.getObject("object_id", UUID::class.java) },
        )
    }

    fun deleteById(objectId: UUID) {
        val params = MapSqlParameterSource().addValue("objectId", objectId)
        jdbc.update(deleteByIdSql, params)
    }

    fun deleteByIdsReturningObjectIds(objectIds: Collection<UUID>): List<UUID> {
        if (objectIds.isEmpty()) return emptyList()
        val params = MapSqlParameterSource().addValue("objectIds", objectIds)
        return jdbc.query(deleteByIdsSql, params) { rs, _ -> rs.getObject("object_id", UUID::class.java) }
    }

    private val incrementNotSeenCountSql =
        """
        UPDATE users
        SET not_seen_count = not_seen_count + 1
        WHERE object_id IN (:objectIds)
        """.trimIndent()

    fun incrementNotSeenCount(objectIds: Collection<UUID>) {
        if (objectIds.isEmpty()) return
        val params = MapSqlParameterSource().addValue("objectIds", objectIds)
        jdbc.update(incrementNotSeenCountSql, params)
    }

    fun getCount(): Int =
        jdbc.queryForObject(
            countAllSql,
            MapSqlParameterSource(),
            Int::class.java,
        ) ?: 0

    fun findStaleObjectIdsWithNotSeenCountGreaterThan(
        cutoff: Instant,
        minNotSeenCount: Int,
    ): List<UUID> {
        val params =
            MapSqlParameterSource()
                .addValue("cutoff", cutoff.atOffset(ZoneOffset.UTC))
                .addValue("minNotSeenCount", minNotSeenCount)

        return jdbc.query(
            findStaleWithNotSeenCountGreaterThanSql,
            params,
        ) { rs, _ -> rs.getObject("object_id", UUID::class.java) }
    }
}
