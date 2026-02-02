package no.novari.msgraphgateway.azure
import com.microsoft.graph.models.OnPremisesExtensionAttributes
import com.microsoft.graph.models.User
import no.novari.msgraphgateway.config.ConfigUser
import org.slf4j.LoggerFactory
import java.io.Serializable

data class AzureUser(
    val mail: String? = null,
    val id: String? = null,
    val userPrincipalName: String? = null,
    var employeeId: String? = null,
    var studentId: String? = null,
    val idpUserObjectId: String? = null,
    val accountEnabled: Boolean? = null,
    val validatorAttribute: String? = null
) : Serializable {

    constructor(user: User, configUser: ConfigUser) : this(
        mail = user.mail,
        id = user.id,
        userPrincipalName = user.userPrincipalName,
        idpUserObjectId = user.id,
        accountEnabled = user.accountEnabled
    ) {
        if (!configUser.useSameIdNumAttribute) {
            employeeId = getAttributeValue(user, configUser.employeeidattribute)
            studentId = getAttributeValue(user, configUser.studentidattribute)

            return
        }

        val valAttrValue = getAttributeValue(user, configUser.validatorAttribute) ?: return
        val userIdNumValue = getAttributeValue(user, configUser.userIdNumAttribute)

        if (configUser.employeeValidator?.let(valAttrValue::contains) == true) {
            employeeId = userIdNumValue
        } else if (configUser.studentValidator?.let(valAttrValue::contains) == true) {
            studentId = userIdNumValue
        }

    }


    companion object {
        private val log = LoggerFactory.getLogger(AzureUser::class.java)

        fun getAttributeValue(user: User, attributeName: String?): String? {
            if (attributeName == null) return null

            val attributeParts = attributeName.split(".")
            return if (attributeParts[0] == "onPremisesExtensionAttributes") {
                val attributeValues: OnPremisesExtensionAttributes? =
                    user.onPremisesExtensionAttributes
                try {
                    attributeValues?.backingStore?.get(attributeParts[1]) as String
                } catch (e: NullPointerException) {
                    log.debug(
                        "getAttributeValue expected {}, but this is not found: {}",
                        attributeName,
                        e.message
                    )
                    return null
                }
            } else {
                user.backingStore.get(attributeName) as String
            }
        }
    }
}
