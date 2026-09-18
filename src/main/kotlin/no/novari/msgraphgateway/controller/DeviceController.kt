package no.novari.msgraphgateway.controller

import no.fintlabs.util.OnlyDevelopers
import no.novari.msgraphgateway.device.MsGraphDevice
import no.novari.msgraphgateway.services.device.DeviceService
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/admin/devices")
class DeviceController(
    private val msGraphDevice: MsGraphDevice,
    private val deviceService: DeviceService,
) {
    @OnlyDevelopers
    @PostMapping("/full-sync")
    fun triggerDeviceFullSync(): ResponseEntity<TriggerResponse> {
        deviceService.triggerFullImport(false)
        return ResponseEntity
            .status(HttpStatus.ACCEPTED)
            .body(TriggerResponse("Devices full sync triggered"))
    }

    @OnlyDevelopers
    @PostMapping("/delta-sync")
    fun triggerDeviceDeltaSync(): ResponseEntity<TriggerResponse> {
        msGraphDevice.pullAllDevicesDelta()
        return ResponseEntity
            .status(HttpStatus.ACCEPTED)
            .body(TriggerResponse("Devices delta sync triggered"))
    }

    @OnlyDevelopers
    @PostMapping("/republish-and-full-sync")
    fun triggerRepublishAndFullSync(): ResponseEntity<TriggerResponse> {
        deviceService.triggerFullImport(true)
        return ResponseEntity
            .status(HttpStatus.ACCEPTED)
            .body(TriggerResponse("Republish and full sync triggered"))
    }
}
