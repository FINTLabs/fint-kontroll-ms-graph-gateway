package no.novari.msgraphgateway.membership.device

import no.novari.kafka.producing.ParameterizedProducerRecord
import no.novari.kafka.producing.ParameterizedTemplate
import no.novari.kafka.producing.ParameterizedTemplateFactory
import no.novari.kafka.topic.EventTopicService
import no.novari.kafka.topic.configuration.EventCleanupFrequency
import no.novari.kafka.topic.configuration.EventTopicConfiguration
import no.novari.kafka.topic.name.EventTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class EntraMembershipProducer(
    private val properties: DeviceMembershipProcessingProperties,
    parameterizedTemplateFactory: ParameterizedTemplateFactory,
    private val entityTopicService: EventTopicService,
) {
    private val template: ParameterizedTemplate<EntraDeviceMembership> =
        parameterizedTemplateFactory.createTemplate(EntraDeviceMembership::class.java)

    private val nameParams: EventTopicNameParameters =
        EventTopicNameParameters
            .builder()
            .eventName("entra-device-group-membership")
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
        log.info("Initialized topic entra-device-group-membership with {} partitions", properties.resultTopicPartitions)
    }

    fun publish(
        messageKey: String,
        entraDeviceMembership: EntraDeviceMembership,
    ) {
        val record =
            ParameterizedProducerRecord
                .builder<EntraDeviceMembership>()
                .topicNameParameters(nameParams)
                .key(messageKey)
                .value(entraDeviceMembership)
                .build()

        template.send(record)
        log.info("Published entra-device-group-membership for messageKey: {}", messageKey)
    }

    companion object {
        private val log = LoggerFactory.getLogger(EntraMembershipProducer::class.java)
    }
}
