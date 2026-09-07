package no.novari.msgraphgateway.repository.user

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import no.novari.msgraphgateway.repository.membership.GroupMembershipStateRepository
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.core.io.ClassPathResource
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator
import org.springframework.transaction.support.TransactionTemplate
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.util.Locale
import java.util.UUID
import kotlin.time.measureTime

@Tag("manual")
@Testcontainers
class UserMembershipDatabasePerformanceTest {
    @Test
    fun `measure database time for 10000 user memberships`() {
        val config =
            HikariConfig().apply {
                jdbcUrl = postgres.jdbcUrl
                username = postgres.username
                password = postgres.password
                maximumPoolSize = 2
            }

        HikariDataSource(config).use { dataSource ->
            ResourceDatabasePopulator(
                ClassPathResource("db/migration/V4__device_memberships.sql"),
                ClassPathResource("db/migration/V5__add_groups_table.sql"),
                ClassPathResource("db/migration/V6__user_memberships.sql"),
                ClassPathResource("db/migration/V7__consolidate_group_membership_state.sql"),
            ).execute(dataSource)

            val jdbc = JdbcTemplate(dataSource)
            val repository = GroupMembershipStateRepository(jdbc)
            val transaction = TransactionTemplate(DataSourceTransactionManager(dataSource))
            val groupId = UUID.randomUUID()
            val memberships = List(MEMBERSHIP_COUNT) { UserMembershipId(UUID.randomUUID(), groupId) }
            jdbc.update(
                "INSERT INTO groups (object_id, resource_group_id, checksum) VALUES (?, ?, ?)",
                groupId,
                12345L,
                byteArrayOf(1),
            )

            val warmupRun = UUID.randomUUID()
            repository.markUsersSeen(warmupRun, memberships.take(1000))
            transaction.executeWithoutResult { repository.completeSnapshot(warmupRun) }
            jdbc.execute("TRUNCATE user_memberships")

            val firstRun = UUID.randomUUID()
            report("Snapshot: insert", MEMBERSHIP_COUNT) {
                repository.markUsersSeen(firstRun, memberships)
            }
            report("Snapshot: complete (including commit)", MEMBERSHIP_COUNT) {
                transaction.executeWithoutResult { repository.completeSnapshot(firstRun) }
            }
            assertEquals(MEMBERSHIP_COUNT, count(jdbc, "observed_present IS TRUE AND desired_present IS NULL"))

            val nextRun = UUID.randomUUID()
            report("Snapshot: update existing", MEMBERSHIP_COUNT) {
                repository.markUsersSeen(nextRun, memberships)
            }
            report("Snapshot: complete unchanged (including commit)", MEMBERSHIP_COUNT) {
                transaction.executeWithoutResult { repository.completeSnapshot(nextRun) }
            }
            assertEquals(
                MEMBERSHIP_COUNT,
                requireNotNull(
                    jdbc.queryForObject(
                        "SELECT COUNT(*) FROM user_memberships WHERE observed_run_id = ?",
                        Int::class.java,
                        nextRun,
                    ),
                ),
            )

            var readCount = 0
            report("Snapshot: read all pages", MEMBERSHIP_COUNT) {
                repository.forEachSnapshotUserAddition(nextRun, true) { readCount++ }
            }
            assertEquals(MEMBERSHIP_COUNT, readCount)

            jdbc.update("UPDATE user_memberships SET desired_present = FALSE, status = 'FAILED'")
            report("Delta: confirm REMOVE and delete (including commit)", MEMBERSHIP_COUNT) {
                transaction.executeWithoutResult { repository.removeObservedUsers(memberships) }
            }
            assertEquals(0, count(jdbc, "TRUE"))

            report("Delta: insert observed memberships", MEMBERSHIP_COUNT) {
                repository.addObservedUsers(memberships)
            }
            assertEquals(MEMBERSHIP_COUNT, count(jdbc, "observed_present IS TRUE AND desired_present IS NULL"))
        }
    }

    private fun count(
        jdbc: JdbcTemplate,
        predicate: String,
    ): Int = requireNotNull(jdbc.queryForObject("SELECT COUNT(*) FROM user_memberships WHERE $predicate", Int::class.java))

    private fun report(
        operation: String,
        count: Int,
        block: () -> Unit,
    ) {
        val elapsed = measureTime(block)
        val milliseconds = elapsed.inWholeNanoseconds / 1_000_000.0
        val perSecond = count * 1_000_000_000.0 / elapsed.inWholeNanoseconds.coerceAtLeast(1)
        println(
            String.format(
                Locale.ROOT,
                "DB TIMING | %s | %d memberships | %.1f ms | %.0f memberships/s",
                operation,
                count,
                milliseconds,
                perSecond,
            ),
        )
    }

    companion object {
        private const val MEMBERSHIP_COUNT = 10_000

        @Container
        @JvmField
        val postgres = PostgreSQLContainer<Nothing>("postgres:16-alpine")
    }
}
