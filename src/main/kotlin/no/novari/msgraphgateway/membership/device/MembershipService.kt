package no.novari.msgraphgateway.membership.device

import com.microsoft.graph.models.ReferenceCreate
import com.microsoft.graph.serviceclient.GraphServiceClient
import com.microsoft.kiota.ApiException
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.OffsetDateTime
import java.util.UUID

@Service
class MembershipService(
    private val graphServiceClient: GraphServiceClient,
    private val entraMembershipProducer: EntraMembershipProducer,
    private val deviceMembershipEntityRepository: DeviceMembershipEntityRepository,
) {
    @Transactional
    fun sendMembershipToEntra(
        messageKey: String,
        deviceResourceGroupMembership: DeviceResourceGroupMembership?,
    ): EntraStatus {
        if (deviceResourceGroupMembership == null) {
            log.warn("Received null membership for key: $messageKey")
            return EntraStatus.ERROR
        }
        val membershipId = parseMembershipId(deviceResourceGroupMembership, messageKey) ?: return EntraStatus.ERROR

        val existing = deviceMembershipEntityRepository.findByIdForUpdate(membershipId)

        if (shouldSkipOperation(existing, deviceResourceGroupMembership.operation)) {
            val kafkaStatus = EntraStatus.NO_CHANGES
            val persistedStatus = normalizePersistedStatus(deviceResourceGroupMembership.operation, kafkaStatus)
            saveMembershipState(membershipId, existing, persistedStatus)
            publishResult(messageKey, deviceResourceGroupMembership, kafkaStatus)
            log.info(
                "Skipped duplicate membership operation {} for device {} and group {}",
                deviceResourceGroupMembership.operation,
                deviceResourceGroupMembership.entraDeviceRef,
                deviceResourceGroupMembership.entraGroupRef,
            )
            return kafkaStatus
        }

        val status = executeWithImmediateRetries(deviceResourceGroupMembership)
        val persistedStatus = normalizePersistedStatus(deviceResourceGroupMembership.operation, status)
        val updated = saveMembershipState(membershipId, existing, persistedStatus)
        log.info(
            "Saved membership state for device {} and group {} with status {} at {}",
            updated.id.deviceRef,
            updated.id.resourceRef,
            updated.status,
            updated.lastUpdatedAt,
        )
        publishResult(messageKey, deviceResourceGroupMembership, status)
        return status
    }

    private fun executeWithImmediateRetries(membership: DeviceResourceGroupMembership): EntraStatus {
        var retryCount = 0
        var status: EntraStatus

        do {
            status =
                when (membership.operation) {
                    OperationType.ADD -> addMembership(membership)
                    OperationType.REMOVE -> removeMembership(membership)
                }

            if (status != EntraStatus.FAILED || retryCount >= MAX_IMMEDIATE_RETRIES) {
                return status
            }

            retryCount++
            log.warn(
                "Membership operation {} failed, retrying immediately ({}/{}) for device {} and group {}",
                membership.operation,
                retryCount,
                MAX_IMMEDIATE_RETRIES,
                membership.entraDeviceRef,
                membership.entraGroupRef,
            )
        } while (true)
    }

    private fun parseMembershipId(
        membership: DeviceResourceGroupMembership,
        messageKey: String,
    ): DeviceMembershipId? =
        try {
            DeviceMembershipId(
                UUID.fromString(membership.entraDeviceRef),
                UUID.fromString(membership.entraGroupRef),
            )
        } catch (_: IllegalArgumentException) {
            log.error(
                "Invalid device/group UUIDs in membership message key {} (deviceRef={}, groupRef={})",
                messageKey,
                membership.entraDeviceRef,
                membership.entraGroupRef,
            )
            publishResult(messageKey, membership, EntraStatus.ERROR)
            null
        }

    private fun shouldSkipOperation(
        existing: DeviceMembershipEntity?,
        operation: OperationType,
    ): Boolean {
        if (existing == null) {
            return false
        }

        return when (operation) {
            OperationType.ADD -> existing.status == EntraStatus.ADDED
            OperationType.REMOVE -> existing.status == EntraStatus.REMOVED
        }
    }

    private fun normalizePersistedStatus(
        operation: OperationType,
        status: EntraStatus,
    ): EntraStatus {
        if (status != EntraStatus.NO_CHANGES) {
            return status
        }

        return when (operation) {
            OperationType.ADD -> EntraStatus.ADDED
            OperationType.REMOVE -> EntraStatus.REMOVED
        }
    }

    private fun saveMembershipState(
        id: DeviceMembershipId,
        existing: DeviceMembershipEntity?,
        status: EntraStatus,
    ): DeviceMembershipEntity {
        val now = OffsetDateTime.now()
        val toSave =
            DeviceMembershipEntity(
                id = id,
                status = status,
                createdAt = existing?.createdAt ?: now,
                lastUpdatedAt = now,
            )
        return deviceMembershipEntityRepository.save(toSave)
    }

    private fun publishResult(
        messageKey: String,
        membership: DeviceResourceGroupMembership,
        status: EntraStatus,
    ) {
        val entraDeviceMembership =
            EntraDeviceMembership(
                status,
                membership.entraGroupRef,
                membership.entraDeviceRef,
            )
        entraMembershipProducer.publish(messageKey, entraDeviceMembership)
    }

    private fun removeMembership(deviceResourceGroupMembership: DeviceResourceGroupMembership): EntraStatus {
        val deviceId = deviceResourceGroupMembership.entraDeviceRef
        val groupId = deviceResourceGroupMembership.entraGroupRef
        try {
            graphServiceClient
                .groups()
                .byGroupId(groupId)
                .members()
                .byDirectoryObjectId(deviceId)
                .ref()
                .delete()
            return EntraStatus.REMOVED
        } catch (apiException: ApiException) {
            if (apiException.responseStatusCode == 404) {
                log.warn(
                    "Produced message to Kafka on deleted DeviceId: {} from GroupId: {} as device not found in group",
                    deviceId,
                    groupId,
                )
                return EntraStatus.ERROR
            } else {
                log.error(
                    "HTTP Error while trying to remove device {} from group {}. Exception: {} \r{}",
                    deviceId,
                    groupId,
                    apiException.responseStatusCode,
                    apiException.message,
                )
                return EntraStatus.FAILED
            }
        } catch (e: Exception) {
            log.error("Failed to process function deleteGroupMembership, Error: ", e)
            return EntraStatus.FAILED
        }
    }

    private fun addMembership(deviceResourceGroupMembership: DeviceResourceGroupMembership): EntraStatus {
        val referenceMember = ReferenceCreate()
        referenceMember.odataId =
            "https://graph.microsoft.com/v1.0/directoryObjects/" + deviceResourceGroupMembership.entraDeviceRef
        try {
            graphServiceClient
                .groups()
                .byGroupId(deviceResourceGroupMembership.entraGroupRef)
                .members()
                .ref()
                .post(referenceMember)
        } catch (apiException: ApiException) {
            if (apiException.responseStatusCode == 400) {
                if (apiException.message?.contains("object references already exist") ?: false) {
                    log.warn(
                        "Device with ID {} already a member of group with ID {}",
                        deviceResourceGroupMembership.entraDeviceRef,
                        deviceResourceGroupMembership.entraGroupRef,
                    )
                    return EntraStatus.NO_CHANGES
                }
                if (apiException.message?.contains("Request_ResourceNotFound") ?: false) {
                    log.warn("Device with ID {} not found in tenant", deviceResourceGroupMembership.entraDeviceRef)
                    return EntraStatus.ERROR
                }
            }

            if (apiException.responseStatusCode == 404) {
                log.warn(
                    "DeviceId: {} cannot be added to GroupId: {}. GroupId and/or DeviceId is not found in tenant",
                    deviceResourceGroupMembership.entraDeviceRef,
                    deviceResourceGroupMembership.entraGroupRef,
                )
                return EntraStatus.ERROR
            }
            if (apiException.responseStatusCode == 429) {
                log.warn("Throttling limit. Error: {}", apiException.message)
                return EntraStatus.FAILED
            } else {
                log.warn(
                    "HTTP Error while updating group {}: {} \r",
                    deviceResourceGroupMembership.entraGroupRef,
                    apiException.message,
                )
                return EntraStatus.FAILED
            }
        }
        return EntraStatus.ADDED
    }

    companion object {
        private val log = LoggerFactory.getLogger(MembershipService::class.java)
        private const val MAX_IMMEDIATE_RETRIES = 2
    }
}
