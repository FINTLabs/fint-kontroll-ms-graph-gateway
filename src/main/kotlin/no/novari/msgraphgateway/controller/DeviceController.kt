package no.novari.msgraphgateway.controller

import no.fintlabs.util.OnlyDevelopers
import no.novari.msgraphgateway.device.MsGraphDevice
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/admin/device")
class DeviceController(
    private val msGraphDevice: MsGraphDevice,
) {
    @OnlyDevelopers
    @GetMapping("/triggers/devicefullsync")
    fun triggerDeviceFullSync(): ResponseEntity<Any> {
        msGraphDevice.fullImportDevices()
        return ResponseEntity
            .status(HttpStatus.ACCEPTED)
            .body(TriggerResponse("Devices Full sync triggered"))
    }

    @OnlyDevelopers
    @GetMapping("/triggers/devicedeltasync")
    fun triggerDeviceDeltaSync(): ResponseEntity<Any> {
        msGraphDevice.pullAllDevicesDelta()
        return ResponseEntity
            .status(HttpStatus.ACCEPTED)
            .body(TriggerResponse("Devices delta sync triggered"))
    }
}
