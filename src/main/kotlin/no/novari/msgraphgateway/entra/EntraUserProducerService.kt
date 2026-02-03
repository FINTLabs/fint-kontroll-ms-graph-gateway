package no.novari.msgraphgateway.azure

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
class EntraUserProducerService(
    private val parameterizedTemplateFactory: ParameterizedTemplateFactory,
    entityTopicService: EntityTopicService,
) {
    private val entraUserTemplate: ParameterizedTemplate<EntraUser> by lazy {
        parameterizedTemplateFactory.createTemplate(EntraUser::class.java)
    }

    private val entityTopicNameParameters: EntityTopicNameParameters

    init {
        val topicNamePrefixParameters =
            TopicNamePrefixParameters
                .stepBuilder()
                .orgIdCommon()
                .domainContextCommon()
                .build()

        entityTopicNameParameters =
            EntityTopicNameParameters
                .builder()
                .topicNamePrefixParameters(topicNamePrefixParameters)
                .resourceName("azure-user")
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
                .builder<EntraUser>()
                .topicNameParameters(entityTopicNameParameters)
                .key(entraUser.idpUserObjectId)
                .value(entraUser)
                .build(),
        )
    }

    fun publishDeletedUser(userId: String) {
        entraUserTemplate.send(
            ParameterizedProducerRecord
                .builder<EntraUser>()
                .topicNameParameters(entityTopicNameParameters)
                .key(userId)
                .value(null)
                .build(),
        )
    }
}
