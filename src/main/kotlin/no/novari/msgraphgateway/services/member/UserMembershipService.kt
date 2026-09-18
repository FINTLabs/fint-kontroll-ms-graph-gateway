package no.novari.msgraphgateway.services.member

import com.microsoft.graph.serviceclient.GraphServiceClient
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import no.novari.msgraphgateway.dto.EntraUserMembershipDto
import no.novari.msgraphgateway.entra.EntraStatus
import no.novari.msgraphgateway.kafka.OperationType
import no.novari.msgraphgateway.kafka.membership.EntraUserMembershipProducer
import no.novari.msgraphgateway.membership.MembershipProcessingProperties
import no.novari.msgraphgateway.membership.user.UserResourceGroupMembership
import no.novari.msgraphgateway.repository.user.UserMembershipEntity
import no.novari.msgraphgateway.repository.user.UserMembershipEntityRepository
import no.novari.msgraphgateway.repository.user.UserMembershipId
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.OffsetDateTime
import java.util.UUID

@Service
class UserMembershipService(
    graphServiceClient: GraphServiceClient,
    private val entraMembershipProducer: EntraUserMembershipProducer,
    private val userMembershipEntityRepository: UserMembershipEntityRepository,
    properties: MembershipProcessingProperties,
    defaultDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : AbstractMembershipService<UserResourceGroupMembership, UserMembershipId, UserMembershipEntity>(
        graphServiceClient,
        properties,
        defaultDispatcher,
    ) {
    @Transactional
    fun deleteAllMemberships(): Int = userMembershipEntityRepository.deleteAll()

    @Transactional
    fun deleteMembershipsUpdatedBefore(cutoff: OffsetDateTime): Int =
        userMembershipEntityRepository.deleteLastUpdatedBefore(cutoff)

    override val memberType = "user"
    override val memberTypeForLog = "User"

    override fun operationOf(membership: UserResourceGroupMembership): OperationType = membership.operation

    override fun groupIdOf(membership: UserResourceGroupMembership): String = membership.entraGroupRef

    override fun memberIdOf(membership: UserResourceGroupMembership): String = membership.userGroupRef

    override fun membershipIdOf(
        memberId: UUID,
        groupId: UUID,
    ): UserMembershipId = UserMembershipId(memberId, groupId)

    override fun findExistingMemberships(
        ids: Collection<UserMembershipId>,
    ): Map<UserMembershipId, UserMembershipEntity> = userMembershipEntityRepository.findAllByIds(ids)

    override fun statusOf(existing: UserMembershipEntity): EntraStatus? =
        when (existing.observedPresent) {
            true -> EntraStatus.ADDED
            false -> EntraStatus.REMOVED
            null -> existing.status
        }

    override fun buildMembershipState(
        id: UserMembershipId,
        existing: UserMembershipEntity?,
        operation: OperationType,
        status: EntraStatus,
    ): UserMembershipEntity {
        val now = OffsetDateTime.now()
        return UserMembershipEntity(
            id = id,
            status = status,
            desiredPresent = operation == OperationType.ADD,
            observedPresent = existing?.observedPresent,
            createdAt = existing?.createdAt ?: now,
            lastUpdatedAt = now,
        )
    }

    override fun saveMembershipStates(states: Collection<UserMembershipEntity>) {
        userMembershipEntityRepository.saveAll(states)
    }

    override fun publishResult(
        messageKey: String,
        membership: UserResourceGroupMembership,
        status: EntraStatus,
    ) {
        entraMembershipProducer.publish(
            messageKey,
            EntraUserMembershipDto(
                status,
                membership.entraGroupRef,
                membership.userGroupRef,
            ),
        )
    }
}
