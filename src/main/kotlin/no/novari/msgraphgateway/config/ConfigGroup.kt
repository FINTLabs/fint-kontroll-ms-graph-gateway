package no.novari.msgraphgateway.config

import jakarta.annotation.PostConstruct
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.ConfigurationProperties
import kotlin.reflect.full.memberProperties

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
    var minNotSeenCount: Int,
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

    @PostConstruct
    fun dumpConfig() {
        val log = LoggerFactory.getLogger(ConfigGroup::class.java)

        this::class
            .memberProperties
            .sortedBy { it.name }
            .forEach { property ->
                val value = property.getter.call(this)
                if (value != null) {
                    log.debug("{}={}", property.name, value)
                }
            }
    }
}
