package no.novari.msgraphgateway.entra

import no.novari.kafka.producing.ParameterizedProducerRecord
import no.novari.kafka.producing.ParameterizedTemplate
import no.novari.kafka.producing.ParameterizedTemplateFactory
import no.novari.kafka.topic.EntityTopicService
import no.novari.kafka.topic.configuration.EntityCleanupFrequency
import no.novari.kafka.topic.configuration.EntityTopicConfiguration
import no.novari.kafka.topic.name.EntityTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import org.springframework.stereotype.Service
import java.time.Duration

@Service
class EntraUserExternalProducerService(
    private val parameterizedTemplateFactory: ParameterizedTemplateFactory,
    entityTopicService: EntityTopicService,
) {
    private val entraUserExternalTemplate: ParameterizedTemplate<EntraUserExternal> by lazy {
        parameterizedTemplateFactory.createTemplate(EntraUserExternal::class.java)
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
                .resourceName("azure-user-external")
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
                .builder<EntraUserExternal>()
                .topicNameParameters(entityTopicNameParameters)
                .key(entraUserExternal.userObjectId)
                .value(entraUserExternal)
                .build(),
        )
    }

    fun publishDeletedUser(userId: String) {
        entraUserExternalTemplate.send(
            ParameterizedProducerRecord
                .builder<EntraUserExternal>()
                .topicNameParameters(entityTopicNameParameters)
                .key(userId)
                .value(null)
                .build(),
        )
    }
}
