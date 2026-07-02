package no.novari.msgraphgateway.services.group

import no.novari.msgraphgateway.entra.EntraStatus
import no.novari.msgraphgateway.entra.group.EntraGroup
import no.novari.msgraphgateway.kafka.group.GroupProducerService
import no.novari.msgraphgateway.repository.group.GroupRepository
import no.novari.msgraphgateway.repository.group.GroupStateRepository
import no.novari.msgraphgateway.services.ChecksumService
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Instant
import java.util.UUID

@Service
class EntraGroupStateService(
    private val groupRepository: GroupRepository,
    private val checksumService: ChecksumService,
    private val groupProducerService: GroupProducerService,
) {
    fun isUnchanged(entraGroup: EntraGroup): Boolean {
        val objectId = parseObjectIdOrNull(entraGroup.objectId) ?: return false
        val storedChecksum = groupRepository.findChecksumById(objectId) ?: return false
        val incomingChecksum = checksumService.checksum(entraGroup)

        return storedChecksum.bytes.contentEquals(incomingChecksum.bytes)
    }

    fun findObjectIdByResourceGroupId(resourceGroupId: String): String? {
        val parsedResourceGroupId = resourceGroupId.toLongOrNull() ?: return null

        return groupRepository
            .findObjectIdByResourceGroupId(parsedResourceGroupId)
            ?.toString()
    }

    fun storeAndPublishIfChanged(
        entraGroup: EntraGroup,
        traceId: String? = null,
        status: EntraStatus = entraGroup.status ?: EntraStatus.CREATED,
    ): Boolean {
        val prepared = prepareUpsert(entraGroup, traceId, status) ?: return false

        val changedIds = groupRepository.batchUpsertReturningChanged(listOf(prepared.row))

        if (changedIds.isEmpty()) {
            log.info("Entra group {} is unchanged; not publishing", prepared.objectId)
            return false
        }

        groupProducerService.publish(prepared.groupToPublish)

        log.debug(
            "Published Entra group {} for ResourceGroupId {} with traceId {}",
            prepared.objectId,
            prepared.resourceGroupId,
            prepared.groupToPublish.traceId,
        )
        return true
    }

    fun storeAndPublish(
        entraGroup: EntraGroup,
        traceId: String? = null,
        status: EntraStatus = entraGroup.status ?: EntraStatus.CREATED,
    ): Boolean {
        val prepared = prepareUpsert(entraGroup, traceId, status) ?: return false

        groupRepository.batchUpsert(listOf(prepared.row))
        groupProducerService.publish(prepared.groupToPublish)

        log.debug(
            "Published Entra group {} for ResourceGroupId {} with traceId {}",
            prepared.objectId,
            prepared.resourceGroupId,
            prepared.groupToPublish.traceId,
        )
        return true
    }

    fun deleteAndPublish(
        objectId: String,
        resourceGroupId: Long? = null,
        traceId: String? = null,
    ): Boolean {
        val uuid =
            parseObjectIdOrNull(objectId)
                ?: run {
                    log.warn("Cannot delete Entra group state with invalid objectId {}", objectId)
                    return false
                }

        groupRepository.deleteById(uuid)
        groupProducerService.publishDeletedGroup(
            groupId = objectId,
            resourceGroupId = resourceGroupId,
            traceId = traceId,
        )

        log.info("Published deleted Entra group {}", objectId)
        return true
    }

    private fun parseObjectIdOrNull(objectId: String?): UUID? =
        if (objectId.isNullOrBlank()) null else runCatching { UUID.fromString(objectId) }.getOrNull()

    private fun prepareUpsert(
        entraGroup: EntraGroup,
        traceId: String?,
        status: EntraStatus,
    ): PreparedUpsert? {
        val objectId =
            parseObjectIdOrNull(entraGroup.objectId)
                ?: run {
                    log.warn("Cannot store Entra group with invalid objectId {}", entraGroup.objectId)
                    return null
                }

        val resourceGroupId =
            entraGroup.resourceGroupID
                ?: run {
                    log.warn("Cannot store Entra group {} without resourceGroupId", objectId)
                    return null
                }

        val groupToPublish =
            entraGroup.copy(
                traceId = traceId.takeUnless { it.isNullOrBlank() } ?: entraGroup.traceId,
                status = status,
            )

        val row =
            GroupStateRepository.UpsertRow(
                objectId = objectId,
                resourceGroupId = resourceGroupId,
                checksum = checksumService.checksum(groupToPublish.copy(traceId = null, status = null)),
                lastSeenAt = Instant.now(),
            )

        return PreparedUpsert(
            objectId = objectId,
            resourceGroupId = resourceGroupId,
            groupToPublish = groupToPublish,
            row = row,
        )
    }

    private data class PreparedUpsert(
        val objectId: UUID,
        val resourceGroupId: Long,
        val groupToPublish: EntraGroup,
        val row: GroupStateRepository.UpsertRow,
    )

    companion object {
        private val log = LoggerFactory.getLogger(EntraGroupStateService::class.java)
    }
}
