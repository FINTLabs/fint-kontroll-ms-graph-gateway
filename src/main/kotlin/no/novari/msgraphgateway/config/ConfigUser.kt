package no.novari.msgraphgateway.config

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

    fun allAttributes(): List<String> {
        val allAttribs = mutableListOf<String>()

        val same = useSameIdNumAttribute == true
        if (!same) {
            studentidattribute?.let { allAttribs.add(it) }
            employeeidattribute?.let { allAttribs.add(it) }
        } else {
            userIdNumAttribute?.let { allAttribs.add(it) }
            validatorAttribute?.let { allAttribs.add(it) }
        }

        if (!mainorgunitidattribute.isNullOrEmpty()) allAttribs.add(mainorgunitidattribute!!)
        if (!mainorgunitnameattribute.isNullOrEmpty()) allAttribs.add(mainorgunitnameattribute!!)
        if (!externaluserattribute.isNullOrEmpty()) allAttribs.add(externaluserattribute!!)

        allAttribs.addAll(userAttributes)
        return allAttribs
    }

    fun userAttributesDelta(): Array<String> {
        var wantsOnPremExtChild = false

        val raw = allAttributes()
        val cleaned = mutableListOf<String>()

        for (s0 in raw) {
            val s = s0.trim()
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
