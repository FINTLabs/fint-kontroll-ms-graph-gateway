package no.novari.msgraphgateway.kafka

import no.novari.kafka.producing.ParameterizedProducerRecord
import no.novari.kafka.producing.ParameterizedTemplate
import no.novari.kafka.producing.ParameterizedTemplateFactory
import no.novari.kafka.topic.EntityTopicService
import no.novari.kafka.topic.configuration.EntityCleanupFrequency
import no.novari.kafka.topic.configuration.EntityTopicConfiguration
import no.novari.kafka.topic.name.EntityTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import no.novari.msgraphgateway.entra.EntraUser
import no.novari.msgraphgateway.entra.EntraUserPayload
import org.springframework.stereotype.Service
import java.time.Duration

@Service
class UserProducerService(
    private val parameterizedTemplateFactory: ParameterizedTemplateFactory,
    entityTopicService: EntityTopicService,
) {
    private val entraUserTemplate: ParameterizedTemplate<EntraUserPayload> by lazy {
        parameterizedTemplateFactory.createTemplate(EntraUserPayload::class.java)
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
                .resourceName("graph-user")
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

    fun publish(entraUser: EntraUser) {
        entraUserTemplate.send(
            ParameterizedProducerRecord
                .builder<EntraUserPayload>()
                .topicNameParameters(entityTopicNameParameters)
                .key(entraUser.userObjectId)
                .value(entraUser.toPayload())
                .build(),
        )
    }

    fun publishDeletedUser(userId: String) {
        entraUserTemplate.send(
            ParameterizedProducerRecord
                .builder<EntraUserPayload>()
                .topicNameParameters(entityTopicNameParameters)
                .key(userId)
                .value(null)
                .build(),
        )
    }
}
