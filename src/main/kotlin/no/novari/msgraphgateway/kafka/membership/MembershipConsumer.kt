package no.novari.msgraphgateway.kafka.membership

import no.novari.kafka.consuming.ErrorHandlerConfiguration
import no.novari.kafka.consuming.ErrorHandlerFactory
import no.novari.kafka.consuming.ListenerConfiguration
import no.novari.kafka.consuming.ParameterizedListenerContainerFactoryService
import no.novari.kafka.topic.EventTopicService
import no.novari.kafka.topic.configuration.EventCleanupFrequency
import no.novari.kafka.topic.configuration.EventTopicConfiguration
import no.novari.kafka.topic.name.EventTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import no.novari.msgraphgateway.membership.device.DeviceMembershipProcessingProperties
import no.novari.msgraphgateway.membership.device.DeviceResourceGroupMembership
import no.novari.msgraphgateway.services.member.MembershipService
import org.springframework.context.annotation.Bean
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class MembershipConsumer(
    private val parameterizedListenerContainerFactoryService: ParameterizedListenerContainerFactoryService,
    private val errorHandlerFactory: ErrorHandlerFactory,
    private val membershipService: MembershipService,
    private val properties: DeviceMembershipProcessingProperties,
    private val eventTopicService: EventTopicService,
) {
    private fun listenerConfiguration() =
        ListenerConfiguration
            .stepBuilder()
            .groupIdApplicationDefault()
            .maxPollRecords(properties.consumerMaxPollRecords)
            .maxPollIntervalKafkaDefault()
            .continueFromPreviousOffsetOnAssignment()
            .build()

    private val topic: EventTopicNameParameters =
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

    init {
        eventTopicService.createOrModifyTopic(
            topic,
            EventTopicConfiguration
                .stepBuilder()
                .partitions(1)
                .retentionTime(Duration.ofDays(7))
                .cleanupFrequency(EventCleanupFrequency.NORMAL)
                .build(),
        )
    }

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
            ).createContainer(topic)
}
