package no.novari.msgraphgateway.kafka.membership

import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import no.novari.kafka.consuming.ErrorHandlerConfiguration
import no.novari.kafka.consuming.ErrorHandlerFactory
import no.novari.kafka.consuming.ListenerConfiguration
import no.novari.kafka.consuming.ParameterizedListenerContainerFactory
import no.novari.kafka.consuming.ParameterizedListenerContainerFactoryService
import no.novari.kafka.topic.EventTopicService
import no.novari.kafka.topic.configuration.EventTopicConfiguration
import no.novari.kafka.topic.name.EventTopicNameParameters
import no.novari.msgraphgateway.kafka.OperationType
import no.novari.msgraphgateway.membership.device.DeviceMembershipProcessingProperties
import no.novari.msgraphgateway.membership.device.DeviceResourceGroupMembership
import no.novari.msgraphgateway.services.member.MembershipService
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.DefaultErrorHandler
import java.time.Duration
import java.util.function.Consumer

class MembershipConsumerTest {
    private val listenerContainerFactoryService = mockk<ParameterizedListenerContainerFactoryService>()
    private val errorHandlerFactory = mockk<ErrorHandlerFactory>()
    private val membershipService = mockk<MembershipService>(relaxed = true)
    private val eventTopicService = mockk<EventTopicService>()
    private val errorHandler = mockk<DefaultErrorHandler>()
    private val listenerFactory = mockk<ParameterizedListenerContainerFactory<DeviceResourceGroupMembership>>()
    private val container = mockk<ConcurrentMessageListenerContainer<String, DeviceResourceGroupMembership>>()
    private val properties =
        DeviceMembershipProcessingProperties(
            consumerMaxPollRecords = 500,
            graphMaxConcurrentCalls = 3,
            graphBatchSize = 20,
            resultTopicPartitions = 1,
            directoryObjectsBaseUrl = "https://graph.microsoft.com/v1.0/directoryObjects/",
        )

    @Test
    fun `constructor creates membership input topic so missing topic on first startup is recovered`() {
        every {
            eventTopicService.createOrModifyTopic(
                any<EventTopicNameParameters>(),
                any<EventTopicConfiguration>(),
            )
        } just Runs

        membershipConsumer()

        verify(exactly = 1) {
            eventTopicService.createOrModifyTopic(
                match {
                    it.eventName == MEMBERSHIP_EVENT_NAME
                },
                match {
                    it.partitions == 1 &&
                        it.retentionTime == Duration.ofDays(7)
                },
            )
        }
    }

    @Test
    fun `container listens on membership event topic and forwards batches`() {
        val batch =
            listOf(
                ConsumerRecord(
                    "topic",
                    0,
                    0,
                    "membership-1",
                    DeviceResourceGroupMembership(
                        operation = OperationType.ADD,
                        entraGroupRef = "11111111-1111-1111-1111-111111111111",
                        entraDeviceRef = "22222222-2222-2222-2222-222222222222",
                    ),
                ),
            )
        val batchListener = slot<Consumer<List<ConsumerRecord<String, DeviceResourceGroupMembership>>>>()

        every {
            eventTopicService.createOrModifyTopic(
                any<EventTopicNameParameters>(),
                any<EventTopicConfiguration>(),
            )
        } just Runs
        every {
            errorHandlerFactory.createErrorHandler(any<ErrorHandlerConfiguration<DeviceResourceGroupMembership>>())
        } returns errorHandler
        every {
            listenerContainerFactoryService.createBatchListenerContainerFactory(
                DeviceResourceGroupMembership::class.java,
                capture(batchListener),
                any<ListenerConfiguration>(),
                errorHandler,
            )
        } returns listenerFactory
        every { listenerFactory.createContainer(any<EventTopicNameParameters>()) } returns container

        val result = membershipConsumer().kontrollMembershipConsumer()
        batchListener.captured.accept(batch)

        assertSame(container, result)
        verify(exactly = 1) {
            listenerFactory.createContainer(
                match<EventTopicNameParameters> {
                    it.eventName == MEMBERSHIP_EVENT_NAME
                },
            )
            membershipService.processKontrollMembershipBatch(batch)
        }
    }

    private fun membershipConsumer(): MembershipConsumer =
        MembershipConsumer(
            parameterizedListenerContainerFactoryService = listenerContainerFactoryService,
            errorHandlerFactory = errorHandlerFactory,
            membershipService = membershipService,
            properties = properties,
            eventTopicService = eventTopicService,
        )

    companion object {
        private const val MEMBERSHIP_EVENT_NAME = "resource-group-membership-device"
    }
}
