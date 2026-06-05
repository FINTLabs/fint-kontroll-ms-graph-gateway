package no.novari.msgraphgateway.kafka.membership

import no.novari.kafka.producing.ParameterizedProducerRecord
import no.novari.kafka.producing.ParameterizedTemplate
import no.novari.kafka.producing.ParameterizedTemplateFactory
import no.novari.kafka.topic.EventTopicService
import no.novari.kafka.topic.configuration.EventCleanupFrequency
import no.novari.kafka.topic.configuration.EventTopicConfiguration
import no.novari.kafka.topic.name.EventTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import no.novari.msgraphgateway.dto.EntraUserMembershipDto
import no.novari.msgraphgateway.membership.MembershipProcessingProperties
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class EntraUserMembershipProducer(
    private val properties: MembershipProcessingProperties,
    parameterizedTemplateFactory: ParameterizedTemplateFactory,
    private val entityTopicService: EventTopicService,
) {
    private val template: ParameterizedTemplate<EntraUserMembershipDto> =
        parameterizedTemplateFactory.createTemplate(EntraUserMembershipDto::class.java)

    private val nameParams: EventTopicNameParameters =
        EventTopicNameParameters
            .builder()
            .eventName("graph-user-group-membership")
            .topicNamePrefixParameters(
                TopicNamePrefixParameters
                    .stepBuilder()
                    .orgIdApplicationDefault()
                    .domainContextApplicationDefault()
                    .build(),
            ).build()

    @EventListener(ApplicationReadyEvent::class)
    fun initializeTopicOnStartup() {
        entityTopicService.createOrModifyTopic(
            nameParams,
            EventTopicConfiguration
                .stepBuilder()
                .partitions(properties.resultTopicPartitions)
                .retentionTime(Duration.ofDays(7))
                .cleanupFrequency(EventCleanupFrequency.NORMAL)
                .build(),
        )
        log.info("Initialized topic graph-user-group-membership with {} partitions", properties.resultTopicPartitions)
    }

    fun publish(
        messageKey: String,
        entraUserMembershipDto: EntraUserMembershipDto,
    ) {
        val record =
            ParameterizedProducerRecord
                .builder<EntraUserMembershipDto>()
                .topicNameParameters(nameParams)
                .key(messageKey)
                .value(entraUserMembershipDto)
                .build()

        template.send(record)
        log.info("Published graph-user-group-membership for messageKey: {}", messageKey)
    }

    companion object {
        private val log = LoggerFactory.getLogger(EntraUserMembershipProducer::class.java)
    }
}
