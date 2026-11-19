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

        assertEquals("-pre-app-test.2233-44-suff-", displayName)
    }

    @Test
    fun `buildDisplayName uses first three characters of resource type`() {
        val mapper =
            mapper(
                filterMode = ConfigGroup.FilterMode.NONE,
                prefix = null,
                suffix = null,
            )

        val displayName = mapper.buildDisplayName(resourceGroup().copy(resourceType = "Device"))

        assertEquals("dev-test.2233-44", displayName)
    }

    @Test
    fun `buildDisplayName omits missing resource type`() {
        val mapper =
            mapper(
                filterMode = ConfigGroup.FilterMode.NONE,
                prefix = "pre-",
                suffix = "-suff",
            )

        val displayName = mapper.buildDisplayName(resourceGroup().copy(resourceType = null))

        assertEquals("pre-test.2233-44-suff", displayName)
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

        assertEquals("app-test.2233-44", displayName)
    }

    @Test
    fun `buildDisplayName uses lowercase and removes unsupported characters`() {
        val mapper =
            mapper(
                filterMode = ConfigGroup.FilterMode.NONE,
                prefix = null,
                suffix = null,
            )

        val displayName = mapper.buildDisplayName(resourceGroup().copy(resourceName = "Test Group.Name_ÆØÅ 123-Ok!"))

        assertEquals("app-test.group.name.aoa.123-ok", displayName)
    }

    @Test
    fun `buildDisplayName transliterates Norwegian characters`() {
        val mapper =
            mapper(
                filterMode = ConfigGroup.FilterMode.NONE,
                prefix = null,
                suffix = null,
            )

        val displayName = mapper.buildDisplayName(resourceGroup().copy(resourceName = "Blåbærsyltetøy"))

        assertEquals("app-blabarsyltetoy", displayName)
    }

    @Test
    fun `buildDisplayName replaces underscores in resource name with dots`() {
        val mapper =
            mapper(
                filterMode = ConfigGroup.FilterMode.NONE,
                prefix = null,
                suffix = null,
            )

        val displayName = mapper.buildDisplayName(resourceGroup().copy(resourceName = "Test_Group_Name"))

        assertEquals("app-test.group.name", displayName)
    }

    @Test
    fun `buildDisplayName collapses repeated and mixed separators`() {
        val mapper =
            mapper(
                filterMode = ConfigGroup.FilterMode.NONE,
                prefix = "pre-",
                suffix = "-suff",
            )

        val displayName = mapper.buildDisplayName(resourceGroup().copy(resourceName = "Test__Group.-.-Name---Here"))

        assertEquals("pre-app-test.group.name-here-suff", displayName)
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
        assertEquals("-pre-app-test.2233-44-suff-", group.displayName)
        assertEquals(223344L, group.resourceGroupId)
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
            resourceName = "test 2233 - 44",
            resourceType = "Application",
        )
}
