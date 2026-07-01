package no.novari.msgraphgateway.kafka.group

import no.novari.kafka.producing.ParameterizedProducerRecord
import no.novari.kafka.producing.ParameterizedTemplate
import no.novari.kafka.producing.ParameterizedTemplateFactory
import no.novari.kafka.topic.EntityTopicService
import no.novari.kafka.topic.configuration.EntityCleanupFrequency
import no.novari.kafka.topic.configuration.EntityTopicConfiguration
import no.novari.kafka.topic.name.EntityTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import no.novari.msgraphgateway.entra.EntraStatus
import no.novari.msgraphgateway.entra.group.EntraGroup
import no.novari.msgraphgateway.entra.group.EntraGroupPayload
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Duration

@Service
class GroupProducerService(
    private val parameterizedTemplateFactory: ParameterizedTemplateFactory,
    entityTopicService: EntityTopicService,
) {
    private val entraGroupTemplate: ParameterizedTemplate<EntraGroupPayload> by lazy {
        parameterizedTemplateFactory.createTemplate(EntraGroupPayload::class.java)
    }

    private val entityTopicNameParameters: EntityTopicNameParameters

    init {
        val topicNamePrefixParameters =
            TopicNamePrefixParameters
                .stepBuilder()
                .orgIdApplicationDefault()
                .domainContextApplicationDefault()
                .build()

        entityTopicNameParameters =
            EntityTopicNameParameters
                .builder()
                .topicNamePrefixParameters(topicNamePrefixParameters)
                .resourceName("graph-group")
                .build()

        entityTopicService.createOrModifyTopic(
            entityTopicNameParameters,
            EntityTopicConfiguration
                .stepBuilder()
                .partitions(1)
                .lastValueRetentionTime(Duration.ofDays(30))
                .nullValueRetentionTime(Duration.ofDays(7))
                .cleanupFrequency(EntityCleanupFrequency.NORMAL)
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
                .topicNameParameters(entityTopicNameParameters)
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
                .topicNameParameters(entityTopicNameParameters)
                .key(groupId)
                .value(payload)
                .build(),
        )
    }

    companion object {
        private val log = LoggerFactory.getLogger(GroupProducerService::class.java)
    }
}
