package no.novari.msgraphgateway.repository.device

import no.novari.msgraphgateway.entra.EntraStatus
import no.novari.msgraphgateway.repository.membership.GroupMembershipStateRepository
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.util.UUID

@Testcontainers(disabledWithoutDocker = true)
class DeviceMembershipObservationTest {
    private lateinit var jdbc: JdbcTemplate
    private lateinit var observations: GroupMembershipStateRepository
    private val groupId = UUID.randomUUID()

    @BeforeEach
    fun setUp() {
        jdbc = JdbcTemplate(DriverManagerDataSource(postgres.jdbcUrl, postgres.username, postgres.password))
        jdbc.execute("DROP TABLE IF EXISTS user_memberships, device_memberships, groups")
        jdbc.execute("CREATE TABLE groups (object_id UUID PRIMARY KEY)")
        for ((table, member) in listOf("user_memberships" to "user_ref", "device_memberships" to "device_ref")) {
            jdbc.execute(
                """
                CREATE TABLE $table (
                    $member UUID NOT NULL, group_ref UUID NOT NULL, status VARCHAR(50),
                    desired_present BOOLEAN, observed_present BOOLEAN, observed_run_id UUID,
                    last_observed_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL,
                    last_updated_at TIMESTAMPTZ NOT NULL, PRIMARY KEY ($member, group_ref)
                )
                """.trimIndent(),
            )
        }
        jdbc.update("INSERT INTO groups VALUES (?)", groupId)
        observations = GroupMembershipStateRepository(jdbc)
    }

    @Test
    fun `snapshot marks an observed device as added`() {
        val membership = DeviceMembershipId(UUID.randomUUID(), groupId)
        val runId = UUID.randomUUID()

        observations.markDevicesSeen(runId, listOf(membership))
        observations.completeSnapshot(runId)

        assertObservedDeviceIsAdded()
    }

    @Test
    fun `delta marks an observed device as added`() {
        val membership = DeviceMembershipId(UUID.randomUUID(), groupId)

        observations.addObservedDevices(listOf(membership))

        assertObservedDeviceIsAdded()
    }

    private fun assertObservedDeviceIsAdded() {
        assertEquals(
            EntraStatus.ADDED.name,
            jdbc.queryForObject("SELECT status FROM device_memberships", String::class.java),
        )
        assertEquals(true, jdbc.queryForObject("SELECT observed_present FROM device_memberships", Boolean::class.java))
    }

    companion object {
        @Container
        @JvmField
        val postgres = PostgreSQLContainer<Nothing>("postgres:16-alpine")
    }
}
