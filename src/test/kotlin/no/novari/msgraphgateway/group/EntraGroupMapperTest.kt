package no.novari.msgraphgateway.group

import no.novari.msgraphgateway.config.ConfigGroup
import no.novari.msgraphgateway.kafka.group.ResourceGroup
import no.novari.msgraphgateway.kafka.group.ResourceGroupOperation
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class EntraGroupMapperTest {
    @Test
    fun `buildDisplayName applies prefix and suffix regardless of filter mode`() {
        val mapper =
            mapper(
                filterMode = ConfigGroup.FilterMode.SUFFIX,
                prefix = "-pre-",
                suffix = "-suff-",
            )

        val displayName = mapper.buildDisplayName(resourceGroup())

        assertEquals("-pre-test223344-suff-", displayName)
    }

    @Test
    fun `buildDisplayName omits missing prefix and suffix`() {
        val mapper =
            mapper(
                filterMode = ConfigGroup.FilterMode.NONE,
                prefix = null,
                suffix = null,
            )

        val displayName = mapper.buildDisplayName(resourceGroup())

        assertEquals("test223344", displayName)
    }

    @Test
    fun `expectedFromResourceGroup uses configured display name`() {
        val mapper =
            mapper(
                filterMode = ConfigGroup.FilterMode.SUFFIX,
                prefix = "-pre-",
                suffix = "-suff-",
            )

        val group = mapper.expectedFromResourceGroup(resourceGroup())

        assertEquals("b4d78b5c-9d57-41f1-8b09-31224442c1ac", group.objectId)
        assertEquals("-pre-test223344-suff-", group.displayName)
        assertEquals(223344L, group.resourceGroupID)
    }

    private fun mapper(
        filterMode: ConfigGroup.FilterMode,
        prefix: String?,
        suffix: String?,
    ): EntraGroupMapper =
        EntraGroupMapper(
            ConfigGroup(
                resourceGroupIdAttribute = "extension_resourceGroupId",
                prefix = prefix,
                suffix = suffix,
                filterMode = filterMode,
                minNotSeenCount = 3,
            ),
        )

    private fun resourceGroup(): ResourceGroup =
        ResourceGroup(
            operation = ResourceGroupOperation.UPDATE,
            resourceId = "223344",
            idpGroupObjectId = "b4d78b5c-9d57-41f1-8b09-31224442c1ac",
            resourceName = "test223344",
        )
}
