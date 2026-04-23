package no.novari.msgraphgateway.entra

import com.microsoft.graph.models.User
import no.novari.msgraphgateway.config.ConfigUser
import java.io.Serializable

data class EntraUserExternal(
    val firstName: String? = null,
    val lastName: String? = null,
    val mobilePhone: String? = null,
    val email: String? = null,
    val userName: String? = null,
    val userObjectId: String? = null,
    val userPrincipalName: String? = null,
    val accountEnabled: Boolean? = null,
    var mainOrganisationUnitName: String? = null,
    var mainOrganisationUnitId: String? = null,
) : Serializable {
    constructor(user: User, configUser: ConfigUser) : this(
        userObjectId = user.id,
        userPrincipalName = user.userPrincipalName,
        accountEnabled = user.accountEnabled,
        firstName = user.givenName,
        lastName = user.surname,
        mobilePhone = user.mobilePhone,
        email = user.mail,
        userName = user.mail,
    ) {
        val additionalData = user.additionalData
        if (!additionalData.isNullOrEmpty()) {
            configUser.mainorgunitnameattribute?.let { key ->
                additionalData[key]?.let { mainOrganisationUnitName = it.toString() }
            }
            configUser.mainorgunitidattribute?.let { key ->
                additionalData[key]?.let { mainOrganisationUnitId = it.toString() }
            }
        }
    }

    fun toPayload(): EntraUserExternalPayload =
        EntraUserExternalPayload(
            userPrincipalName = userPrincipalName,
            accountEnabled = accountEnabled,
            firstName = firstName,
            lastName = lastName,
            email = email,
            userName = userName,
            mainOrganisationUnitName = mainOrganisationUnitName,
            mainOrganisationUnitId = mainOrganisationUnitId,
        )
}
