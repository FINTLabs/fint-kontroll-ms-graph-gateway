package no.novari.msgraphgateway.membership.device

import no.novari.kafka.consuming.ErrorHandlerConfiguration
import no.novari.kafka.consuming.ErrorHandlerFactory
import no.novari.kafka.consuming.ListenerConfiguration
import no.novari.kafka.consuming.ParameterizedListenerContainerFactoryService
import no.novari.kafka.topic.name.EventTopicNameParameters
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.context.annotation.Bean
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
            .maxPollRecords(properties.consumerMaxPollRecords)
            .maxPollIntervalKafkaDefault()
            .continueFromPreviousOffsetOnAssignment()
            .build()

    val topic: EventTopicNameParameters? =
        EventTopicNameParameters
            .builder()
            .eventName("kontroll-resource-group-membership-device")
            .build()

    @Bean
    fun kontrollMembershipConsumer() =
        parameterizedListenerContainerFactoryService
            .createRecordListenerContainerFactory(
                DeviceResourceGroupMembership::class.java,
                { record: ConsumerRecord<String, DeviceResourceGroupMembership> ->
                    membershipService.sendMembershipToEntra(record.key(), record.value())
                },
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
