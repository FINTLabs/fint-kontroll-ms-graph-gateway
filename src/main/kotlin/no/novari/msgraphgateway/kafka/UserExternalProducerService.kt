package no.novari.msgraphgateway.kafka

import no.novari.kafka.producing.ParameterizedProducerRecord
import no.novari.kafka.producing.ParameterizedTemplate
import no.novari.kafka.producing.ParameterizedTemplateFactory
import no.novari.kafka.topic.EntityTopicService
import no.novari.kafka.topic.configuration.EntityCleanupFrequency
import no.novari.kafka.topic.configuration.EntityTopicConfiguration
import no.novari.kafka.topic.name.EntityTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import no.novari.msgraphgateway.entra.EntraUserExternal
import no.novari.msgraphgateway.entra.EntraUserExternalPayload
import org.springframework.stereotype.Service
import java.time.Duration

@Service
class UserExternalProducerService(
    private val parameterizedTemplateFactory: ParameterizedTemplateFactory,
    entityTopicService: EntityTopicService,
) {
    private val entraUserExternalTemplate: ParameterizedTemplate<EntraUserExternalPayload> by lazy {
        parameterizedTemplateFactory.createTemplate(EntraUserExternalPayload::class.java)
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
                .resourceName("graph-user-external")
                .build()

        entityTopicService.createOrModifyTopic(
            entityTopicNameParameters,
            EntityTopicConfiguration
                .stepBuilder()
                .partitions(1)
                .lastValueRetainedForever()
                .nullValueRetentionTime(Duration.ofDays(7))
                .cleanupFrequency(EntityCleanupFrequency.NORMAL)
                .build(),
        )
    }

    fun publish(entraUserExternal: EntraUserExternal) {
        entraUserExternalTemplate.send(
            ParameterizedProducerRecord
                .builder<EntraUserExternalPayload>()
                .topicNameParameters(entityTopicNameParameters)
                .key(entraUserExternal.userObjectId)
                .value(entraUserExternal.toPayload())
                .build(),
        )
    }

    fun publishDeletedUser(userId: String) {
        entraUserExternalTemplate.send(
            ParameterizedProducerRecord
                .builder<EntraUserExternalPayload>()
                .topicNameParameters(entityTopicNameParameters)
                .key(userId)
                .value(null)
                .build(),
        )
    }
}
