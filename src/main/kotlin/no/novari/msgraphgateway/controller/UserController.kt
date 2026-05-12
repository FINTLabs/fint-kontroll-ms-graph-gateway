package no.novari.msgraphgateway.controller

import no.fintlabs.util.OnlyDevelopers
import no.novari.msgraphgateway.service.GroupService
import no.novari.msgraphgateway.service.UserService
import no.novari.msgraphgateway.user.MsGraphUser
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

data class ErrorResponse(
    val error: String,
    val objectId: String? = null,
)

data class TriggerResponse(
    val message: String,
)

@RestController
@RequestMapping("/api/admin/users")
class UserController(
    private val groupService: GroupService,
    private val msGraphUser: MsGraphUser,
    private val userService: UserService,
) {
    @OnlyDevelopers
    @GetMapping("/{objectId}")
    fun getUserWithGroups(
        @PathVariable objectId: String,
    ): ResponseEntity<*> {
        val dto = groupService.getEntraUserWithGroups(objectId)

        return if (dto.user == null) {
            ResponseEntity
                .status(HttpStatus.NOT_FOUND)
                .body(ErrorResponse("User not found", objectId))
        } else {
            ResponseEntity.ok(dto)
        }
    }

    @OnlyDevelopers
    @PostMapping("/full-sync")
    fun triggerUserFullSync(): ResponseEntity<TriggerResponse> {
        userService.triggerFullImport(false)
        return ResponseEntity
            .status(HttpStatus.ACCEPTED)
            .body(TriggerResponse("Users full sync triggered"))
    }

    @OnlyDevelopers
    @PostMapping("/delta-sync")
    fun triggerUserDeltaSync(): ResponseEntity<TriggerResponse> {
        msGraphUser.pullAllUsersDelta()
        return ResponseEntity
            .status(HttpStatus.ACCEPTED)
            .body(TriggerResponse("Users delta sync triggered"))
    }

    @OnlyDevelopers
    @PostMapping("/republish-and-full-sync")
    fun triggerRepublishAndFullSync(): ResponseEntity<TriggerResponse> {
        userService.triggerFullImport(true)
        return ResponseEntity
            .status(HttpStatus.ACCEPTED)
            .body(TriggerResponse("Republish and full sync triggered"))
    }
}
