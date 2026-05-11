package no.novari.msgraphgateway.membership.device

import jakarta.validation.constraints.Max
import jakarta.validation.constraints.Min
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.validation.annotation.Validated

@Validated
@ConfigurationProperties(prefix = "ms-graph.membership.device")
data class DeviceMembershipProcessingProperties(
    @field:Min(1)
    var consumerConcurrency: Int,
    @field:Min(1)
    var consumerMaxPollRecords: Int,
    @field:Min(1)
    @field:Max(3)
    var graphMaxConcurrentCalls: Int,
    @field:Min(1)
    @field:Max(20)
    var graphBatchSize: Int,
    @field:Min(1)
    var resultTopicPartitions: Int,
    var directoryObjectsBaseUrl: String,
)
