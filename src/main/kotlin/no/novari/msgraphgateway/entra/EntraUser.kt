package no.novari.msgraphgateway.azure
import com.microsoft.graph.models.OnPremisesExtensionAttributes
import com.microsoft.graph.models.User
import no.novari.msgraphgateway.config.ConfigUser
import org.slf4j.LoggerFactory
import java.io.Serializable

data class EntraUser(
    val mail: String? = null,
    val id: String? = null,
    val userPrincipalName: String? = null,
    val employeeId: String? = null,
    val studentId: String? = null,
    val idpUserObjectId: String? = null,
    val accountEnabled: Boolean? = null,
    val validatorAttribute: String? = null,
) : Serializable {
    constructor(user: User, configUser: ConfigUser) : this(
        mail = user.mail,
        id = user.id,
        userPrincipalName = user.userPrincipalName,
        idpUserObjectId = user.id,
        accountEnabled = user.accountEnabled,
    ) {
        if (!configUser.useSameIdNumAttribute) {
            setEmployeeAndStudentIds(
                employeeId = getAttributeValue(user, configUser.employeeidattribute),
                studentId = getAttributeValue(user, configUser.studentidattribute),
            )
            return
        }

        val valAttrValue = getAttributeValue(user, configUser.validatorAttribute) ?: return
        val userIdNumValue = getAttributeValue(user, configUser.userIdNumAttribute)

        when {
            valAttrValue.contains(requireNotNull(configUser.employeeValidator)) -> {
                setEmployeeAndStudentIds(employeeId = userIdNumValue, studentId = null)
            }

            valAttrValue.contains(requireNotNull(configUser.studentValidator)) -> {
                setEmployeeAndStudentIds(employeeId = null, studentId = userIdNumValue)
            }
        }
    }

    private fun setEmployeeAndStudentIds(
        employeeId: String?,
        studentId: String?,
    ) {
        val fieldEmployeeId = EntraUser::class.java.getDeclaredField("employeeId")
        fieldEmployeeId.isAccessible = true
        fieldEmployeeId.set(this, employeeId)

        val fieldStudentId = EntraUser::class.java.getDeclaredField("studentId")
        fieldStudentId.isAccessible = true
        fieldStudentId.set(this, studentId)
    }

    companion object {
        private val log = LoggerFactory.getLogger(EntraUser::class.java)

        fun getAttributeValue(
            user: User,
            attributeName: String?,
        ): String? {
            if (attributeName == null) return null

            val attributeParts = attributeName.split(".")
            return if (attributeParts[0] == "onPremisesExtensionAttributes") {
                val attributeValues: OnPremisesExtensionAttributes? =
                    user.onPremisesExtensionAttributes
                try {
                    attributeValues?.backingStore?.get(attributeParts[1])
                } catch (e: NullPointerException) {
                    log.debug(
                        "getAttributeValue expected {}, but this is not found: {}",
                        attributeName,
                        e.message,
                    )
                    null
                }
            } else {
                user.backingStore.get(attributeName) as? String
            }
        }
    }
}
