package no.novari.msgraphgateway.repository.group

import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import no.novari.msgraphgateway.services.Checksum
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.jdbc.core.ConnectionCallback
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.sql.Connection
import java.time.Instant
import java.util.UUID
import java.sql.Array as SqlArray

class GroupsRepositoryTest {
    @Test
    fun `batchUpsert binds checksum bytes as bytea array`() {
        val jdbc = mockk<NamedParameterJdbcTemplate>()
        val jdbcTemplate = mockk<JdbcTemplate>()
        val connection = mockk<Connection>()
        val sqlArray = mockk<SqlArray>()
        val byteaElements = slot<Array<out Any?>>()
        val checksumBytes = byteArrayOf(1, 2, 3)
        val repository = GroupsRepository(jdbc, table = "groups")

        every { jdbc.jdbcTemplate } returns jdbcTemplate
        every { connection.createArrayOf(eq("uuid"), any()) } returns sqlArray
        every { connection.createArrayOf(eq("int8"), any()) } returns sqlArray
        every { connection.createArrayOf(eq("bytea"), capture(byteaElements)) } returns sqlArray
        every { connection.createArrayOf(eq("timestamptz"), any()) } returns sqlArray
        every { jdbc.update(any<String>(), any<MapSqlParameterSource>()) } returns 1
        every { jdbcTemplate.execute(any<ConnectionCallback<Int>>()) } answers {
            firstArg<ConnectionCallback<Int>>().doInConnection(connection)
        }

        repository.batchUpsert(
            listOf(
                GroupStateRepository.UpsertRow(
                    objectId = UUID.randomUUID(),
                    resourceGroupId = 112233,
                    checksum = Checksum(checksumBytes),
                    lastSeenAt = Instant.parse("2026-07-16T08:53:04Z"),
                ),
            ),
        )

        assertTrue(byteaElements.captured.single() is ByteArray)
        assertArrayEquals(checksumBytes, byteaElements.captured.single() as ByteArray)

        verify(exactly = 1) {
            connection.createArrayOf("bytea", any())
        }
    }
}
