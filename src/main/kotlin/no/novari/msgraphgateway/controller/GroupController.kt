package no.novari.msgraphgateway.controller

import no.fintlabs.util.OnlyDevelopers
import no.novari.msgraphgateway.group.MsGraphGroup
import no.novari.msgraphgateway.service.GroupService
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

data class ErrorResponseResourceId(
    val message: String,
)

@RestController
@RequestMapping("/groups")
class GroupController(
    private val msGraphGroup: MsGraphGroup,
    private val groupService: GroupService,
) {
    @OnlyDevelopers
    @PostMapping("/full-sync")
    fun triggerGroupsFullSync(): ResponseEntity<TriggerResponse> {
        groupService.triggerFullImport(false)

        return ResponseEntity
            .status(HttpStatus.ACCEPTED)
            .body(TriggerResponse("Groups full sync triggered"))
    }

    @OnlyDevelopers
    @PostMapping("/delta-sync")
    fun triggerGroupsDeltaSync(): ResponseEntity<TriggerResponse> {
        msGraphGroup.pullAllGroupsDelta()

        return ResponseEntity
            .status(HttpStatus.ACCEPTED)
            .body(TriggerResponse("Groups delta sync triggered"))
    }

    @OnlyDevelopers
    @PostMapping("/republish-and-full-sync")
    fun triggerRepublishAndFullSync(): ResponseEntity<TriggerResponse> {
        groupService.triggerFullImport(true)

        return ResponseEntity
            .status(HttpStatus.ACCEPTED)
            .body(TriggerResponse("Republish and full sync of groups triggered"))
    }

    @OnlyDevelopers
    @GetMapping("/objectid/{groupId}")
    fun getGroupInfo(
        @PathVariable groupId: String,
    ): ResponseEntity<*> {
        val group = groupService.getGroupInfo(groupId)

        return if (group == null) {
            ResponseEntity
                .status(HttpStatus.NOT_FOUND)
                .body(ErrorResponse("Group not found", groupId))
        } else {
            ResponseEntity.ok(group)
        }
    }

    @OnlyDevelopers
    @GetMapping("/resourceid/{id}")
    fun getGroupInfoByResourceGroupId(
        @PathVariable id: Long,
    ): ResponseEntity<*> {
        val group = groupService.getGroupInfoByResourceGroupId(id)

        return if (group == null) {
            ResponseEntity
                .status(HttpStatus.NOT_FOUND)
                .body(ErrorResponseResourceId("Group with resourceID: $id not found in Entra"))
        } else {
            ResponseEntity.ok(group)
        }
    }
}
