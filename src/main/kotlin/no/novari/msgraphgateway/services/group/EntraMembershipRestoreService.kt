package no.novari.msgraphgateway.services.group

import com.microsoft.graph.models.ReferenceCreate
import com.microsoft.graph.serviceclient.GraphServiceClient
import com.microsoft.kiota.ApiException
import no.novari.msgraphgateway.membership.MembershipProcessingProperties
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.UUID

@Service
class EntraMembershipRestoreService(
    private val graphServiceClient: GraphServiceClient,
    private val properties: MembershipProcessingProperties,
) {
    fun restoreUserMembership(
        groupId: UUID,
        userId: UUID,
    ): MembershipRestoreResult = restoreMembership(groupId, userId, "user")

    fun restoreDeviceMembership(
        groupId: UUID,
        deviceId: UUID,
    ): MembershipRestoreResult = restoreMembership(groupId, deviceId, "device")

    private fun restoreMembership(
        groupId: UUID,
        memberId: UUID,
        memberType: String,
    ): MembershipRestoreResult {
        val reference = ReferenceCreate().apply { odataId = properties.directoryObjectsBaseUrl + memberId }
        try {
            graphServiceClient
                .groups()
                .byGroupId(groupId.toString())
                .members()
                .ref()
                .post(reference)
        } catch (exception: ApiException) {
            if (exception.responseStatusCode == 404) {
                log.warn(
                    "Cannot restore {} {} to group {} because the group or object does not exist",
                    memberType,
                    memberId,
                    groupId,
                )
                return MembershipRestoreResult.NOT_POSSIBLE
            }
            if (exception.responseStatusCode == 400 &&
                exception.message?.contains("object references already exist", ignoreCase = true) == true
            ) {
                return MembershipRestoreResult.RESTORED
            }
            if (exception.responseStatusCode in 400..499 && exception.responseStatusCode != 429) {
                log.warn(
                    "Cannot restore {} {} to group {} (status={}), publishing ERROR without retry",
                    memberType,
                    memberId,
                    groupId,
                    exception.responseStatusCode,
                )
                return MembershipRestoreResult.NOT_POSSIBLE
            }
            throw exception
        }

        log.info("Restored {} {} to group {} after membership drift was detected", memberType, memberId, groupId)
        return MembershipRestoreResult.RESTORED
    }

    companion object {
        private val log = LoggerFactory.getLogger(EntraMembershipRestoreService::class.java)
    }
}

enum class MembershipRestoreResult {
    RESTORED,
    NOT_POSSIBLE,
}
