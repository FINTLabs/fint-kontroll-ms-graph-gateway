package no.novari.msgraphgateway.kafka

import no.novari.kafka.producing.ParameterizedProducerRecord
import no.novari.kafka.producing.ParameterizedTemplate
import no.novari.kafka.producing.ParameterizedTemplateFactory
import no.novari.kafka.topic.EntityTopicService
import no.novari.kafka.topic.configuration.EntityCleanupFrequency
import no.novari.kafka.topic.configuration.EntityTopicConfiguration
import no.novari.kafka.topic.name.EntityTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import no.novari.msgraphgateway.entra.EntraDevice
import org.springframework.stereotype.Service
import java.time.Duration

@Service
class EntraDeviceProducerService(
    private val parameterizedTemplateFactory: ParameterizedTemplateFactory,
    entityTopicService: EntityTopicService,
) {
    private val entraDeviceTemplate: ParameterizedTemplate<EntraDevice> by lazy {
        parameterizedTemplateFactory.createTemplate(EntraDevice::class.java)
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
                .resourceName("entra-device")
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

    fun publish(entraDevice: EntraDevice) {
        entraDeviceTemplate.send(
            ParameterizedProducerRecord
                .builder<EntraDevice>()
                .topicNameParameters(entityTopicNameParameters)
                .key(entraDevice.deviceId)
                .value(entraDevice)
                .build(),
        )
    }

    fun publishDeletedDevice(deviceId: String) {
        entraDeviceTemplate.send(
            ParameterizedProducerRecord
                .builder<EntraDevice>()
                .topicNameParameters(entityTopicNameParameters)
                .key(deviceId)
                .value(null)
                .build(),
        )
    }
}
