package no.novari.msgraphgateway.controller

import com.microsoft.graph.serviceclient.GraphServiceClient
import com.microsoft.graph.users.item.getmembergroups.GetMemberGroupsPostRequestBody
import no.novari.msgraphgateway.config.ConfigGroup
import no.novari.msgraphgateway.config.ConfigUser
import no.novari.msgraphgateway.entra.EntraGroup
import no.novari.msgraphgateway.entra.EntraUser
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.server.ResponseStatusException

@Service
class GroupService(
    private val configUser: ConfigUser,
    private val configGroup: ConfigGroup,
    private val graphServiceClient: GraphServiceClient,
) {
    fun getEntraUserWithGroups(userId: String): UserWithGroupsDto =
        try {
            val selection = configUser.userAttributesDelta()
            val user =
                graphServiceClient.users().byUserId(userId).get { req ->
                    req.queryParameters?.select = selection
                }
                    ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "User not found: $userId")

            val entraUser = EntraUser(user, configUser)

            val requestBody =
                GetMemberGroupsPostRequestBody().apply {
                    securityEnabledOnly = true
                }

            val groupIds: List<String> =
                graphServiceClient
                    .users()
                    .byUserId(userId)
                    .getMemberGroups()
                    .post(requestBody)
                    ?.value
                    ?: emptyList()

            val selectionCriteriaGroup =
                arrayOf(
                    "id",
                    "displayName",
                    configGroup.resourceGroupIdAttribute ?: "",
                ).filter { it.isNotBlank() }.toTypedArray()

            val entraGroups = mutableListOf<EntraGroup>()

            for (groupId in groupIds) {
                val group =
                    graphServiceClient
                        .groups()
                        .byGroupId(groupId)
                        .get { requestConfig ->
                            requestConfig.queryParameters?.select = selectionCriteriaGroup
                        } ?: continue

                val displayName = group.displayName
                val additionalData = group.additionalData
                val hasSuffix = configGroup.suffix?.let { s -> displayName?.endsWith(s) == true } ?: false
                val hasResourceAttr =
                    configGroup.resourceGroupIdAttribute
                        ?.let { key -> additionalData.containsKey(key) }
                        ?: false

                if (hasSuffix && hasResourceAttr) {
                    entraGroups += EntraGroup(group, configGroup)
                }
            }

            log.info(
                "*** <<< Rest service found userId {}. User is member of {} FINT kontroll groups >>> ***",
                userId,
                entraGroups.size,
            )

            UserWithGroupsDto(entraUser, entraGroups)
        } catch (ex: Exception) {
            log.error("Failed to fetch user or groups: {}", ex.message)
            UserWithGroupsDto()
        }

    companion object {
        private val log = LoggerFactory.getLogger(GroupService::class.java)
    }
}
