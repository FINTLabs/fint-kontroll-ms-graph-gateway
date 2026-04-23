package no.novari.msgraphgateway.entra
import com.microsoft.graph.models.User
import no.novari.msgraphgateway.config.ConfigUser
import java.io.Serializable

data class EntraUser(
    val mail: String? = null,
    val userPrincipalName: String? = null,
    var employeeId: String? = null,
    var studentId: String? = null,
    val userObjectId: String? = null,
    val accountEnabled: Boolean? = null,
) : Serializable {
    constructor(user: User, configUser: ConfigUser) : this(
        mail = user.mail,
        userPrincipalName = user.userPrincipalName,
        userObjectId = user.id,
        accountEnabled = user.accountEnabled,
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
        fun getAttributeValue(
            user: User,
            attributeName: String?,
        ): String? {
            if (attributeName == null) return null

            val attributeParts = attributeName.split(".")
            return if (attributeParts[0] == "onPremisesExtensionAttributes") {
                user.onPremisesExtensionAttributes
                    ?.backingStore
                    ?.get(attributeParts[1])
            } else {
                user.backingStore.get(attributeName)
            }
        }
    }

    fun toPayload(): EntraUserPayload =
        EntraUserPayload(
            mail = mail,
            userPrincipalName = userPrincipalName,
            employeeId = employeeId,
            studentId = studentId,
            accountEnabled = accountEnabled,
        )
}
