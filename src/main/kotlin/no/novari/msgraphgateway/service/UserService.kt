package no.novari.msgraphgateway.service

import no.novari.msgraphgateway.user.MsGraphUser
import org.springframework.stereotype.Service

@Service
class UserService(
    private val msGraphUser: MsGraphUser,
) {
    fun triggerFullImport(republishAll: Boolean) {
        msGraphUser.requestFullImport(republishAll)
    }
}
