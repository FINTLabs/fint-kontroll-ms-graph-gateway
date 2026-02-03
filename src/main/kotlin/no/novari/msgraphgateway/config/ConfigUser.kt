package no.novari.msgraphgateway.config

import jakarta.annotation.PostConstruct
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component
import java.util.LinkedHashSet

@Component
@ConfigurationProperties(prefix = "fint.kontroll.ms-graph-gateway.user")
class ConfigUser {
    companion object {
        @JvmStatic
        val userAttributes: List<String> =
            listOf(
                "id",
                "accountEnabled",
                "mail",
                "mobilePhone",
                "onPremisesExtensionAttributes",
                "userPrincipalName",
                "displayName",
                "givenName",
                "surname",
                "onPremisesUserPrincipalName",
                "onPremisesSamAccountName",
            )
    }

    var mainorgunitidattribute: String? = null
    var mainorgunitnameattribute: String? = null
    var employeeidattribute: String? = null
    var studentidattribute: String? = null
    var externaluserattribute: String? = null
    var externaluservalue: String? = null
    var userpagingsize: Int? = null
    var enableExternalUsers: Boolean? = null
    var useSameIdNumAttribute: Boolean = false
    var userIdNumAttribute: String? = null
    var studentValidator: String? = null
    var employeeValidator: String? = null
    var validatorAttribute: String? = null
    var staleAfterDays: Int = 7
    var acceptedDeviationPercent: Int? = null
    var minNotSeenCount: Int = 7

    @PostConstruct
    fun dumpConfig() {
        val log = LoggerFactory.getLogger(ConfigUser::class.java)
        log.info("useSameIdNumAttribute={}", useSameIdNumAttribute)
        log.info("employeeidattribute={}", employeeidattribute)
        log.info("studentidattribute={}", studentidattribute)
        log.info("userIdNumAttribute={}", userIdNumAttribute)
        log.info("validatorAttribute={}", validatorAttribute)
    }

    fun allAttributes(): List<String> {
        val allAttributes = mutableListOf<String>()

        if (!useSameIdNumAttribute) {
            studentidattribute?.let { allAttributes.add(it) }
            employeeidattribute?.let { allAttributes.add(it) }
        } else {
            userIdNumAttribute?.let { allAttributes.add(it) }
            validatorAttribute?.let { allAttributes.add(it) }
        }

        if (!mainorgunitidattribute.isNullOrEmpty()) allAttributes.add(mainorgunitidattribute!!)
        if (!mainorgunitnameattribute.isNullOrEmpty()) allAttributes.add(mainorgunitnameattribute!!)
        if (!externaluserattribute.isNullOrEmpty()) allAttributes.add(externaluserattribute!!)

        allAttributes.addAll(userAttributes)
        return allAttributes
    }

    fun userAttributesDelta(): Array<String> {
        var wantsOnPremExtChild = false

        val raw = allAttributes()
        val cleaned = mutableListOf<String>()

        for (attribute in raw) {
            val s = attribute.trim()
            if (s.isEmpty()) continue

            if (s.startsWith("onPremisesExtensionAttributes.")) {
                wantsOnPremExtChild = true
                continue
            }

            if (s.contains(".")) {
                continue
            }

            cleaned.add(s)
        }

        cleaned.addAll(userAttributes)
        cleaned.add("userType")

        if (wantsOnPremExtChild && !cleaned.contains("onPremisesExtensionAttributes")) {
            cleaned.add("onPremisesExtensionAttributes")
        }

        val orderedUnique = LinkedHashSet(cleaned)
        return orderedUnique.toTypedArray()
    }
}
