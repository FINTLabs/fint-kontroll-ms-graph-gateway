package no.novari.msgraphgateway.kafka

import no.novari.msgraphgateway.config.ConfigGroup
import no.novari.msgraphgateway.group.EntraGroupCommandService
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.Optional
import java.util.concurrent.ConcurrentHashMap

@Service
class ResourceGroupConsumerService(
    private val configGroup: ConfigGroup,
    private val entraGroupCommandService: EntraGroupCommandService,
) {
    private val resourceGroupCache =
        ConcurrentHashMap<String, Optional<ResourceGroup>>()

    fun processEntity(
        resourceGroup: ResourceGroup?,
        kafkaKey: String,
    ) {
        log.info("Received resource-group message key={}, delete={}", kafkaKey, resourceGroup == null)

        val next = Optional.ofNullable(resourceGroup)
        val previous = resourceGroupCache[kafkaKey]

        if (previous == next) {
            log.info("Unchanged resource-group message for key={}; skipping", kafkaKey)
            return
        }

        resourceGroupCache[kafkaKey] = next

        if (resourceGroup == null) {
            handleDelete(kafkaKey)
        } else {
            handleUpsert(kafkaKey, resourceGroup)
        }
    }

    private fun handleUpsert(
        kafkaKey: String,
        resourceGroup: ResourceGroup,
    ) {
        if (resourceGroup.resourceName.isNullOrBlank()) {
            log.warn("ResourceGroup {} has no resourceName; skipping", kafkaKey)
            return
        }

        val existingGroupId =
            resourceGroup.identityProviderGroupObjectId
                ?: entraGroupCommandService.findGroupIdByResourceGroupId(resourceGroup.id)

        if (existingGroupId.isNullOrBlank()) {
            log.info("New ResourceGroup detected: {}. Creating group in Entra", resourceGroup.resourceName)
            entraGroupCommandService.createGroup(resourceGroup)
            return
        }

        if (configGroup.allowGroupUpdate == true) {
            log.info("Updating existing Entra group {} for ResourceGroupId {}", existingGroupId, resourceGroup.id)
            entraGroupCommandService.updateGroup(resourceGroup.copy(identityProviderGroupObjectId = existingGroupId))
        } else {
            log.warn("ResourceGroupId {} already exists in Entra, but allowGroupUpdate=false", resourceGroup.id)
        }
    }

    private fun handleDelete(kafkaKey: String) {
        if (configGroup.allowGroupDelete == true) {
            log.info("Deleting Entra group for ResourceGroupId {}", kafkaKey)
            entraGroupCommandService.deleteGroup(kafkaKey)
        } else {
            log.warn("ResourceGroupId {} was NOT deleted because allowGroupDelete=false", kafkaKey)
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(ResourceGroupConsumerService::class.java)
    }
}
