package no.novari.msgraphgateway.kafka.group

import no.novari.kafka.producing.ParameterizedProducerRecord
import no.novari.kafka.producing.ParameterizedTemplate
import no.novari.kafka.producing.ParameterizedTemplateFactory
import no.novari.kafka.topic.EventTopicService
import no.novari.kafka.topic.configuration.EventCleanupFrequency
import no.novari.kafka.topic.configuration.EventTopicConfiguration
import no.novari.kafka.topic.name.EventTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import no.novari.msgraphgateway.entra.EntraStatus
import no.novari.msgraphgateway.entra.group.EntraGroup
import no.novari.msgraphgateway.entra.group.EntraGroupPayload
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class GroupProducerService(
    private val parameterizedTemplateFactory: ParameterizedTemplateFactory,
    private val eventTopicService: EventTopicService,
) {
    private val entraGroupTemplate: ParameterizedTemplate<EntraGroupPayload> by lazy {
        parameterizedTemplateFactory.createTemplate(EntraGroupPayload::class.java)
    }

    private val nameParameters: EventTopicNameParameters

    init {
        val topicNamePrefixParameters =
            TopicNamePrefixParameters
                .stepBuilder()
                .orgIdApplicationDefault()
                .domainContextApplicationDefault()
                .build()

        nameParameters =
            EventTopicNameParameters
                .builder()
                .topicNamePrefixParameters(topicNamePrefixParameters)
                .eventName("graph-group")
                .build()
    }

    @EventListener(ApplicationReadyEvent::class)
    fun initializeTopicOnStartup() {
        eventTopicService.createOrModifyTopic(
            nameParameters,
            EventTopicConfiguration
                .stepBuilder()
                .partitions(1)
                .retentionTime(Duration.ofDays(7))
                .cleanupFrequency(EventCleanupFrequency.NORMAL)
                .build(),
        )
    }

    fun publish(
        entraGroup: EntraGroup,
        status: EntraStatus = entraGroup.status ?: EntraStatus.CREATED,
    ) {
        log.debug(
            "Publishing group to Kafka: objectId={}, displayName={}, traceId={}, status={}",
            entraGroup.objectId,
            entraGroup.displayName,
            entraGroup.traceId,
            status,
        )

        entraGroupTemplate.send(
            ParameterizedProducerRecord
                .builder<EntraGroupPayload>()
                .topicNameParameters(nameParameters)
                .key(entraGroup.objectId.toString())
                .value(entraGroup.toPayload(status))
                .build(),
        )
    }

    fun publishDeletedGroup(
        groupId: String,
        resourceGroupId: Long? = null,
        traceId: String? = null,
    ) {
        log.debug(
            "Publishing deleted group to Kafka: objectId={}, resourceGroupId={}, traceId={}",
            groupId,
            resourceGroupId,
            traceId,
        )

        val payload =
            EntraGroupPayload(
                objectId = groupId,
                displayName = null,
                resourceGroupId = resourceGroupId,
                traceId = traceId,
                status = EntraStatus.DELETED,
            )

        entraGroupTemplate.send(
            ParameterizedProducerRecord
                .builder<EntraGroupPayload>()
                .topicNameParameters(nameParameters)
                .key(groupId)
                .value(payload)
                .build(),
        )
    }

    fun publishResourceGroupResponse(
        key: String,
        objectId: String?,
        displayName: String?,
        resourceGroupId: Long?,
        traceId: String?,
        status: EntraStatus,
    ) {
        log.debug(
            "Publishing resource-group response to Kafka: key={}, objectId={}, resourceGroupId={}, traceId={}, status={}",
            key,
            objectId,
            resourceGroupId,
            traceId,
            status,
        )

        val payload =
            EntraGroupPayload(
                objectId = objectId,
                displayName = displayName,
                resourceGroupId = resourceGroupId,
                traceId = traceId,
                status = status,
            )

        entraGroupTemplate.send(
            ParameterizedProducerRecord
                .builder<EntraGroupPayload>()
                .topicNameParameters(nameParameters)
                .key(key)
                .value(payload)
                .build(),
        )
    }

    companion object {
        private val log = LoggerFactory.getLogger(GroupProducerService::class.java)
    }
}
