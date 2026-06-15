package no.novari.msgraphgateway.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "ms-graph.group")
class ConfigGroup(
    var resourceGroupIdAttribute: String? = null,
    var prefix: String? = null,
    var suffix: String? = null,
    var filterMode: FilterMode = FilterMode.SUFFIX,
    var allowGroupUpdate: Boolean? = null,
    var allowGroupDelete: Boolean? = null,
    var groupPagingSize: Int? = null,
    var uniqueNamePrefix: String? = null,
    var minNotSeenCount: Int = 3,
) {
    enum class FilterMode {
        PREFIX,
        SUFFIX,
        BOTH,
        NONE,
    }

    companion object {
        private val groupAttributes = listOf("id", "displayName")
        private const val MEMBERS_ATTRIBUTE = "members"
    }

    fun getAllGroupAttributes(): Array<String> {
        val allAttribs = mutableListOf<String>()

        allAttribs += groupAttributes
        allAttribs += MEMBERS_ATTRIBUTE
        resourceGroupIdAttribute?.let { allAttribs += it }
        return allAttribs.toTypedArray()
    }

    fun getGroupAttributesNotMembers(): Array<String> {
        val allAttribs = mutableListOf<String>()
        allAttribs += groupAttributes
        resourceGroupIdAttribute?.let { allAttribs += it }
        return allAttribs.toTypedArray()
    }
}
