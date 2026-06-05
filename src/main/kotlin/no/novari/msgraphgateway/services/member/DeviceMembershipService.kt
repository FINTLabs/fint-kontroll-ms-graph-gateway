package no.novari.msgraphgateway.services.member

import com.microsoft.graph.serviceclient.GraphServiceClient
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import no.novari.msgraphgateway.dto.EntraDeviceMembershipDto
import no.novari.msgraphgateway.entra.EntraStatus
import no.novari.msgraphgateway.kafka.OperationType
import no.novari.msgraphgateway.kafka.membership.EntraDeviceMembershipProducer
import no.novari.msgraphgateway.membership.MembershipProcessingProperties
import no.novari.msgraphgateway.membership.device.DeviceResourceGroupMembership
import no.novari.msgraphgateway.repository.device.DeviceMembershipEntity
import no.novari.msgraphgateway.repository.device.DeviceMembershipEntityRepository
import no.novari.msgraphgateway.repository.device.DeviceMembershipId
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.OffsetDateTime
import java.util.UUID

@Service
class DeviceMembershipService(
    graphServiceClient: GraphServiceClient,
    private val entraDeviceMembershipProducer: EntraDeviceMembershipProducer,
    private val deviceMembershipEntityRepository: DeviceMembershipEntityRepository,
    properties: MembershipProcessingProperties,
    defaultDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : AbstractMembershipService<DeviceResourceGroupMembership, DeviceMembershipId, DeviceMembershipEntity>(
        graphServiceClient,
        properties,
        defaultDispatcher,
    ) {
    @Transactional
    fun deleteAllMemberships(): Int = deviceMembershipEntityRepository.deleteAll()

    @Transactional
    fun deleteMembershipsUpdatedBefore(cutoff: OffsetDateTime): Int =
        deviceMembershipEntityRepository.deleteLastUpdatedBefore(cutoff)

    override val memberType = "device"
    override val memberTypeForLog = "Device"

    override fun operationOf(membership: DeviceResourceGroupMembership): OperationType = membership.operation

    override fun groupIdOf(membership: DeviceResourceGroupMembership): String = membership.entraGroupRef

    override fun memberIdOf(membership: DeviceResourceGroupMembership): String = membership.entraDeviceRef

    override fun membershipIdOf(
        memberId: UUID,
        groupId: UUID,
    ): DeviceMembershipId = DeviceMembershipId(memberId, groupId)

    override fun findExistingMemberships(
        ids: Collection<DeviceMembershipId>,
    ): Map<DeviceMembershipId, DeviceMembershipEntity> = deviceMembershipEntityRepository.findAllByIds(ids)

    override fun statusOf(existing: DeviceMembershipEntity): EntraStatus = existing.status

    override fun buildMembershipState(
        id: DeviceMembershipId,
        existing: DeviceMembershipEntity?,
        status: EntraStatus,
    ): DeviceMembershipEntity {
        val now = OffsetDateTime.now()
        return DeviceMembershipEntity(
            id = id,
            status = status,
            createdAt = existing?.createdAt ?: now,
            lastUpdatedAt = now,
        )
    }

    override fun saveMembershipStates(states: Collection<DeviceMembershipEntity>) {
        deviceMembershipEntityRepository.saveAll(states)
    }

    override fun publishResult(
        messageKey: String,
        membership: DeviceResourceGroupMembership,
        status: EntraStatus,
    ) {
        entraDeviceMembershipProducer.publish(
            messageKey,
            EntraDeviceMembershipDto(
                status,
                membership.entraGroupRef,
                membership.entraDeviceRef,
            ),
        )
    }
}
