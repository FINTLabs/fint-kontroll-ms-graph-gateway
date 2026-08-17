package no.novari.msgraphgateway.kafka.group

import no.novari.kafka.consuming.ErrorHandlerConfiguration
import no.novari.kafka.consuming.ErrorHandlerFactory
import no.novari.kafka.consuming.ListenerConfiguration
import no.novari.kafka.consuming.ParameterizedListenerContainerFactoryService
import no.novari.kafka.topic.name.EventTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.context.annotation.Bean
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.stereotype.Component

@Component
class ResourceGroupConsumer(
    private val parameterizedListenerContainerFactoryService: ParameterizedListenerContainerFactoryService,
    private val errorHandlerFactory: ErrorHandlerFactory,
    private val resourceGroupConsumerService: ResourceGroupConsumerService,
) {
    private fun listenerConfiguration() =
        ListenerConfiguration
            .stepBuilder()
            .groupIdApplicationDefault()
            .maxPollRecordsKafkaDefault()
            .maxPollIntervalKafkaDefault()
            .continueFromPreviousOffsetOnAssignment()
            .build()

    private val topic: EventTopicNameParameters =
        EventTopicNameParameters
            .builder()
            .eventName("resource-group")
            .topicNamePrefixParameters(
                TopicNamePrefixParameters
                    .stepBuilder()
                    .orgIdApplicationDefault()
                    .domainContextApplicationDefault()
                    .build(),
            ).build()

    @Bean
    fun resourceGroupConsumerContainer(): ConcurrentMessageListenerContainer<String, ResourceGroup> =
        parameterizedListenerContainerFactoryService
            .createRecordListenerContainerFactory(
                ResourceGroup::class.java,
                { record: ConsumerRecord<String, ResourceGroup> ->
                    resourceGroupConsumerService.process(
                        record.value(),
                        record.key(),
                    )
                },
                listenerConfiguration(),
                errorHandlerFactory.createErrorHandler(
                    ErrorHandlerConfiguration
                        .stepBuilder<ResourceGroup>()
                        .noRetries()
                        .skipFailedRecords()
                        .build(),
                ),
            ).createContainer(topic)
}
