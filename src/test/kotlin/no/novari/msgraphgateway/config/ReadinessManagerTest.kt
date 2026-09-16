package no.novari.msgraphgateway.config

import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.context.ApplicationContext
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class ReadinessManagerTest {
    @Test
    fun `application readiness waits until groups are loaded`() {
        val manager = ReadinessManager(mockk<ApplicationContext>(relaxed = true))
        val executor = Executors.newSingleThreadExecutor()

        try {
            val waiting = executor.submit { manager.waitUntilGroupsAreLoaded() }

            assertFalse(waiting.isDone)

            manager.ready()

            waiting.get(1, TimeUnit.SECONDS)
            assertTrue(waiting.isDone)
        } finally {
            executor.shutdownNow()
        }
    }

    @Test
    fun `application readiness continues immediately when groups finished first`() {
        val manager = ReadinessManager(mockk<ApplicationContext>(relaxed = true))

        manager.ready()

        val executor = Executors.newSingleThreadExecutor()
        try {
            val waiting = executor.submit { manager.waitUntilGroupsAreLoaded() }
            waiting.get(1, TimeUnit.SECONDS)
            assertTrue(waiting.isDone)
        } finally {
            executor.shutdownNow()
        }
    }
}
