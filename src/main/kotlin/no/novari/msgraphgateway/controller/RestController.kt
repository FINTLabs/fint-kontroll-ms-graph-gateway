package no.novari.msgraphgateway.controller

import no.novari.msgraphgateway.device.MsGraphDevice
import no.novari.msgraphgateway.user.MsGraphUser
import org.springframework.http.ResponseEntity
import org.springframework.scheduling.Trigger
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api")
class ApiRestController(
    private val restService: RestService,
    private val msGraphUser: MsGraphUser,
    private val msGraphDevice: MsGraphDevice,
) {
    @GetMapping("/users/{objectId}")
    fun getUserWithGroups(
        @PathVariable objectId: String,
    ): ResponseEntity<Any> {
        val dto = restService.getEntraUserWithGroups(objectId)

        if (dto.user == null) {
            return ResponseEntity.status(404).body(
                mapOf(
                    "error" to "User not found",
                    "objectId" to objectId,
                ),
            )
        }

        return ResponseEntity.ok(dto)
    }

    @GetMapping("/triggers/userfullsync")
    fun triggerUserFullSync(): ResponseEntity<Any> {
        msGraphUser.fullImportUsers()
        return ResponseEntity.ok(
            mapOf(
                "message" to "Users Full sync triggered",
            ),
        )
    }

    @GetMapping("/triggers/userdeltasync")
    fun triggerUserDeltaSync(): ResponseEntity<Any> {
        msGraphUser.pullAllUsersDelta()
        return ResponseEntity.ok(
            mapOf(
                "message" to "Users delta sync triggered",
            ),
        )
    }

    @GetMapping("/triggers/devicefullsync")
    fun triggerDeviceFullSync(): ResponseEntity<Any> {
        msGraphDevice.fullImportDevices()
        return ResponseEntity.ok(
            mapOf(
                "message" to "Devices Full sync triggered",
            ),
        )
    }

    @GetMapping("/triggers/devicedeltasync")
    fun triggerDeviceDeltaSync(): ResponseEntity<Any> {
        msGraphDevice.pullAllDevicesDelta()
        return ResponseEntity.ok(
            mapOf(
                "message" to "Devices delta sync triggered",
            ),
        )
    }
}
