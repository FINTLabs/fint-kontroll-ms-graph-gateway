package no.novari.msgraphgateway.services.device

import no.novari.msgraphgateway.device.MsGraphDevice
import org.springframework.stereotype.Service

@Service
class DeviceService(
    private val msGraphDevice: MsGraphDevice,
) {
    fun triggerFullImport(republishAll: Boolean) {
        msGraphDevice.requestFullImport(republishAll)
    }
}
