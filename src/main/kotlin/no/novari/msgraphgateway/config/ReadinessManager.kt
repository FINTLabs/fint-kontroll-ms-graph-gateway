package no.novari.msgraphgateway.config

import org.springframework.boot.availability.AvailabilityChangeEvent
import org.springframework.boot.availability.ReadinessState
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.boot.context.event.ApplicationStartedEvent
import org.springframework.context.ApplicationContext
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.util.concurrent.CountDownLatch

@Component
class ReadinessManager(
    private val applicationContext: ApplicationContext,
) {
    private val groupsLoaded = CountDownLatch(1)

    @EventListener(ApplicationStartedEvent::class)
    fun startNotReady() {
        AvailabilityChangeEvent.publish(applicationContext, ReadinessState.REFUSING_TRAFFIC)
    }

    @EventListener(ApplicationReadyEvent::class)
    fun waitUntilGroupsAreLoaded() {
        groupsLoaded.await()
    }

    fun ready() {
        groupsLoaded.countDown()
    }
}
