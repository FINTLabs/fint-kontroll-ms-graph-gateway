package no.novari.msgraphgateway.controller

import no.fintlabs.util.OnlyDevelopers
import no.novari.msgraphgateway.device.MsGraphDevice
import no.novari.msgraphgateway.service.DeviceService
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/admin/device")
class DeviceController(
    private val msGraphDevice: MsGraphDevice,
    private val deviceService: DeviceService,
) {
    @OnlyDevelopers
    @PostMapping("/devicefullsync")
    fun triggerDeviceFullSync(): ResponseEntity<Any> {
        deviceService.triggerFullImport(false)
        return ResponseEntity
            .status(HttpStatus.ACCEPTED)
            .body(TriggerResponse("Devices Full sync triggered"))
    }

    @OnlyDevelopers
    @PostMapping("/devicedeltasync")
    fun triggerDeviceDeltaSync(): ResponseEntity<Any> {
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
