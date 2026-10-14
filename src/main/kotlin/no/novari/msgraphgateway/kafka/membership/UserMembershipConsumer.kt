package no.novari.msgraphgateway.kafka.membership

import no.novari.kafka.consuming.ErrorHandlerConfiguration
import no.novari.kafka.consuming.ErrorHandlerFactory
import no.novari.kafka.consuming.ListenerConfiguration
import no.novari.kafka.consuming.ParameterizedListenerContainerFactoryService
import no.novari.kafka.topic.name.EventTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import no.novari.msgraphgateway.membership.MembershipProcessingProperties
import no.novari.msgraphgateway.membership.user.UserResourceGroupMembership
import no.novari.msgraphgateway.services.member.UserMembershipService
import org.springframework.context.annotation.Bean
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.stereotype.Component

@Component
class UserMembershipConsumer(
    private val parameterizedListenerContainerFactoryService: ParameterizedListenerContainerFactoryService,
    private val errorHandlerFactory: ErrorHandlerFactory,
    private val membershipService: UserMembershipService,
    private val properties: MembershipProcessingProperties,
) {
    private fun listenerConfiguration() =
        ListenerConfiguration
            .stepBuilder()
            .groupIdApplicationDefault()
            .maxPollRecords(properties.consumerMaxPollRecords)
            .maxPollIntervalKafkaDefault()
            .continueFromPreviousOffsetOnAssignment()
            .build()

    val topic: EventTopicNameParameters =
        EventTopicNameParameters
            .builder()
            .eventName("resource-group-membership-user")
            .topicNamePrefixParameters(
                TopicNamePrefixParameters
                    .stepBuilder()
                    .orgIdApplicationDefault()
                    .domainContextApplicationDefault()
                    .build(),
            ).build()

    @Bean
    fun kontrollUserMembershipConsumer(): ConcurrentMessageListenerContainer<String, UserResourceGroupMembership> =
        parameterizedListenerContainerFactoryService
            .createBatchListenerContainerFactory(
                UserResourceGroupMembership::class.java,
                { batch -> membershipService.processKontrollMembershipBatch(batch) },
                listenerConfiguration(),
                errorHandlerFactory.createErrorHandler(
                    ErrorHandlerConfiguration
                        .stepBuilder<UserResourceGroupMembership>()
                        .noRetries()
                        .skipFailedRecords()
                        .build(),
                ),
                { container ->
                    container.setConcurrency(properties.consumerConcurrency)
                },
            ).createContainer(topic)
}
