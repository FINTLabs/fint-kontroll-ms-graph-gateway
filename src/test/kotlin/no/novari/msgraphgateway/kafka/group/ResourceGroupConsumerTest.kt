package no.novari.msgraphgateway.kafka.group

import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import no.novari.kafka.consuming.ErrorHandlerConfiguration
import no.novari.kafka.consuming.ErrorHandlerFactory
import no.novari.kafka.consuming.ListenerConfiguration
import no.novari.kafka.consuming.ParameterizedListenerContainerFactory
import no.novari.kafka.consuming.ParameterizedListenerContainerFactoryService
import no.novari.kafka.topic.EventTopicService
import no.novari.kafka.topic.configuration.EventTopicConfiguration
import no.novari.kafka.topic.name.EventTopicNameParameters
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.DefaultErrorHandler
import java.time.Duration
import java.util.function.Consumer

class ResourceGroupConsumerTest {
    private val listenerContainerFactoryService = mockk<ParameterizedListenerContainerFactoryService>()
    private val errorHandlerFactory = mockk<ErrorHandlerFactory>()
    private val resourceGroupConsumerService = mockk<ResourceGroupConsumerService>(relaxed = true)
    private val eventTopicService = mockk<EventTopicService>()
    private val errorHandler = mockk<DefaultErrorHandler>()
    private val listenerFactory = mockk<ParameterizedListenerContainerFactory<ResourceGroup>>()
    private val container = mockk<ConcurrentMessageListenerContainer<String, ResourceGroup>>()

    @Test
    fun `constructor creates resource group input topic so missing topic on first startup is recovered`() {
        every {
            eventTopicService.createOrModifyTopic(
                any<EventTopicNameParameters>(),
                any<EventTopicConfiguration>(),
            )
        } just Runs

        resourceGroupConsumer()

        verify(exactly = 1) {
            eventTopicService.createOrModifyTopic(
                match {
                    it.eventName == RESOURCE_GROUP_EVENT_NAME
                },
                match {
                    it.partitions == 1 &&
                        it.retentionTime == Duration.ofDays(7)
                },
            )
        }
    }

    @Test
    fun `container listens on resource group event topic and forwards records`() {
        val resourceGroup =
            ResourceGroup(
                operation = ResourceGroupOperation.CREATE,
                resourceId = "12345",
                resourceName = "TestGroup",
            )
        val recordListener = slot<Consumer<ConsumerRecord<String, ResourceGroup>>>()

        every {
            eventTopicService.createOrModifyTopic(
                any<EventTopicNameParameters>(),
                any<EventTopicConfiguration>(),
            )
        } just Runs
        every {
            errorHandlerFactory.createErrorHandler(any<ErrorHandlerConfiguration<ResourceGroup>>())
        } returns errorHandler
        every {
            listenerContainerFactoryService.createRecordListenerContainerFactory(
                ResourceGroup::class.java,
                capture(recordListener),
                any<ListenerConfiguration>(),
                errorHandler,
            )
        } returns listenerFactory
        every { listenerFactory.createContainer(any<EventTopicNameParameters>()) } returns container

        val result = resourceGroupConsumer().resourceGroupConsumerContainer()
        recordListener.captured.accept(ConsumerRecord("topic", 0, 0, "trace-123", resourceGroup))

        assertSame(container, result)
        verify(exactly = 1) {
            listenerFactory.createContainer(
                match<EventTopicNameParameters> {
                    it.eventName == RESOURCE_GROUP_EVENT_NAME
                },
            )
            resourceGroupConsumerService.process(resourceGroup, "trace-123")
        }
    }

    private fun resourceGroupConsumer(): ResourceGroupConsumer =
        ResourceGroupConsumer(
            parameterizedListenerContainerFactoryService = listenerContainerFactoryService,
            errorHandlerFactory = errorHandlerFactory,
            resourceGroupConsumerService = resourceGroupConsumerService,
            eventTopicService = eventTopicService,
        )

    companion object {
        private const val RESOURCE_GROUP_EVENT_NAME = "resource-group"
    }
}
