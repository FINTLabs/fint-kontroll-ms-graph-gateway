package no.novari.msgraphgateway.repository.user

import no.novari.msgraphgateway.entra.EntraStatus
import no.novari.msgraphgateway.repository.membership.GroupMembershipStateRepository
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.OffsetDateTime
import java.util.UUID

@Testcontainers
class UserMembershipRemovalTest {
    private lateinit var jdbc: JdbcTemplate
    private lateinit var commands: UserMembershipEntityRepository
    private lateinit var observations: GroupMembershipStateRepository
    private val id = UserMembershipId(UUID.randomUUID(), UUID.randomUUID())

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
        jdbc.update("INSERT INTO groups VALUES (?)", id.groupRef)
        commands = UserMembershipEntityRepository(jdbc)
        observations = GroupMembershipStateRepository(jdbc)
    }

    @Test
    fun `one confirmed remove preserves every other user and group membership`() {
        val otherUser = UserMembershipId(UUID.randomUUID(), id.groupRef)
        val otherGroup = UserMembershipId(id.userRef, UUID.randomUUID())
        val anotherMembership = UserMembershipId(UUID.randomUUID(), UUID.randomUUID())
        val memberships = listOf(id, otherUser, otherGroup, anotherMembership)
        observations.addObservedUsers(memberships)

        commands.saveAll(listOf(state(EntraStatus.REMOVED, false, true)))

        assertEquals(setOf(otherUser, otherGroup, anotherMembership), commands.findAllByIds(memberships).keys)
        assertEquals(3, jdbc.queryForObject("SELECT COUNT(*) FROM user_memberships", Int::class.java))
        assertTrue(commands.findAllByIds(memberships).values.all { it.observedPresent == true })
    }

    @Test
    fun `one delta remove preserves other pending removals`() {
        val otherUser = UserMembershipId(UUID.randomUUID(), id.groupRef)
        observations.addObservedUsers(listOf(id, otherUser))
        jdbc.update("UPDATE user_memberships SET desired_present = FALSE, status = 'FAILED'")

        observations.removeObservedUsers(listOf(id))

        assertEquals(setOf(otherUser), commands.findAllByIds(listOf(id, otherUser)).keys)
        assertEquals(true, commands.findAllByIds(listOf(otherUser)).getValue(otherUser).observedPresent)
    }

    @Test
    fun `confirmed remove deletes even when previous observation said present and replay stays absent`() {
        commands.saveAll(listOf(state(EntraStatus.ADDED, true, true)))
        commands.saveAll(listOf(state(EntraStatus.REMOVED, false, true)))
        assertTrue(commands.findAllByIds(listOf(id)).isEmpty())
        commands.saveAll(listOf(state(EntraStatus.REMOVED, false, null)))
        assertTrue(commands.findAllByIds(listOf(id)).isEmpty())
    }

    @Test
    fun `failed remove is retained until delta confirms absence`() {
        commands.saveAll(listOf(state(EntraStatus.FAILED, false, true)))
        assertEquals(EntraStatus.FAILED, commands.findAllByIds(listOf(id)).getValue(id).status)
        observations.removeObservedUsers(listOf(id))
        assertTrue(commands.findAllByIds(listOf(id)).isEmpty())
    }

    @Test
    fun `snapshot confirms pending remove without a previous observation`() {
        commands.saveAll(listOf(state(EntraStatus.FAILED, false, null)))
        val runId = UUID.randomUUID()
        observations.discardSnapshot(runId)
        assertFalse(commands.findAllByIds(listOf(id)).isEmpty())
        observations.completeSnapshot(runId)
        assertTrue(commands.findAllByIds(listOf(id)).isEmpty())
    }

    @Test
    fun `snapshot keeps pending remove when membership is still present`() {
        commands.saveAll(listOf(state(EntraStatus.FAILED, false, true)))
        val runId = UUID.randomUUID()
        observations.markUsersSeen(runId, listOf(id))
        observations.completeSnapshot(runId)
        assertEquals(false, commands.findAllByIds(listOf(id)).getValue(id).desiredPresent)
        assertEquals(true, commands.findAllByIds(listOf(id)).getValue(id).observedPresent)
    }

    @Test
    fun `absence does not erase a desired add`() {
        commands.saveAll(listOf(state(EntraStatus.ADDED, true, true)))
        observations.removeObservedUsers(listOf(id))
        observations.completeSnapshot(UUID.randomUUID())
        assertEquals(true, commands.findAllByIds(listOf(id)).getValue(id).desiredPresent)
    }

    @Test
    fun `Entra-only membership remains unknown and run id changes with each snapshot`() {
        val first = UUID.randomUUID()
        val second = UUID.randomUUID()
        observations.markUsersSeen(first, listOf(id))
        observations.completeSnapshot(first)
        assertEquals(first, jdbc.queryForObject("SELECT observed_run_id FROM user_memberships", UUID::class.java))
        observations.markUsersSeen(second, listOf(id))
        observations.completeSnapshot(second)
        assertEquals(second, jdbc.queryForObject("SELECT observed_run_id FROM user_memberships", UUID::class.java))
        assertNull(commands.findAllByIds(listOf(id)).getValue(id).desiredPresent)
    }

    private fun state(status: EntraStatus, desired: Boolean, observed: Boolean?) =
        UserMembershipEntity(id, status, desired, observed, OffsetDateTime.now(), OffsetDateTime.now())

    companion object {
        @Container
        @JvmField
        val postgres = PostgreSQLContainer<Nothing>("postgres:16-alpine")
    }
}
