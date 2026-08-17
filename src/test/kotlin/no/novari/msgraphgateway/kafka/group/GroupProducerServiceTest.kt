package no.novari.msgraphgateway.kafka.group

import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import no.novari.kafka.producing.ParameterizedTemplateFactory
import no.novari.kafka.topic.EventTopicService
import no.novari.kafka.topic.configuration.EventTopicConfiguration
import no.novari.kafka.topic.name.EventTopicNameParameters
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.context.event.EventListener

class GroupProducerServiceTest {
    @Test
    fun `initializeTopicOnStartup creates graph group event topic`() {
        val templateFactory = mockk<ParameterizedTemplateFactory>(relaxed = true)
        val eventTopicService = mockk<EventTopicService>()
        val service = GroupProducerService(templateFactory, eventTopicService)

        every {
            eventTopicService.createOrModifyTopic(
                any<EventTopicNameParameters>(),
                any<EventTopicConfiguration>(),
            )
        } just Runs

        service.initializeTopicOnStartup()

        verify(exactly = 1) {
            eventTopicService.createOrModifyTopic(
                any<EventTopicNameParameters>(),
                any<EventTopicConfiguration>(),
            )
        }
    }

    @Test
    fun `initializeTopicOnStartup is registered as Spring event listener`() {
        val method = GroupProducerService::class.java.getDeclaredMethod("initializeTopicOnStartup")

        assertTrue(method.isAnnotationPresent(EventListener::class.java))
    }
}
