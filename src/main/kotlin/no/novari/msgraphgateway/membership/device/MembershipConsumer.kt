package no.novari.msgraphgateway.membership.device

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
class MembershipConsumer(
    private val parameterizedListenerContainerFactoryService: ParameterizedListenerContainerFactoryService,
    private val errorHandlerFactory: ErrorHandlerFactory,
    private val membershipService: MembershipService,
    private val properties: DeviceMembershipProcessingProperties,
) {
    private fun listenerConfiguration() =
        ListenerConfiguration
            .stepBuilder()
            .groupIdApplicationDefault()
            .maxPollRecords(properties.consumerMaxPollRecords) // test with the default value and some smaller ones
            .maxPollIntervalKafkaDefault()
            .continueFromPreviousOffsetOnAssignment()
            .build()

    val topic: EventTopicNameParameters =
        EventTopicNameParameters
            .builder()
            .eventName("resource-group-membership-device")
            .topicNamePrefixParameters(
                TopicNamePrefixParameters
                    .stepBuilder()
                    .orgIdApplicationDefault()
                    .domainContextApplicationDefault()
                    .build(),
            ).build()

    @Bean
    fun kontrollMembershipConsumer(): ConcurrentMessageListenerContainer<String, DeviceResourceGroupMembership> =
        parameterizedListenerContainerFactoryService
            .createBatchListenerContainerFactory(
                DeviceResourceGroupMembership::class.java,
                { batch -> membershipService.processKontrollMembershipBatch(batch) },
                listenerConfiguration(),
                errorHandlerFactory.createErrorHandler(
                    ErrorHandlerConfiguration
                        .stepBuilder<DeviceResourceGroupMembership>()
                        .noRetries()
                        .skipFailedRecords()
                        .build(),
                ),
                { container ->
                    container.setConcurrency(properties.consumerConcurrency)
                },
            ).createContainer(topic)
}
