package no.novari.msgraphgateway.service

import no.novari.msgraphgateway.device.MsGraphDevice
import no.novari.msgraphgateway.user.MsGraphUser
import org.springframework.stereotype.Service

@Service
class DeviceService(
    private val msGraphDevice: MsGraphDevice,
) {
    fun triggerFullImport(republishAll: Boolean) {
        msGraphDevice.requestFullImport(republishAll)
    }
}
