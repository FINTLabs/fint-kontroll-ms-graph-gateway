package no.novari.msgraphgateway.controller

import no.fintlabs.util.OnlyDevelopers
import no.novari.msgraphgateway.group.MsGraphGroup
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

data class GroupWithUniqueNameResponse(
    val id: String?,
    val displayName: String?,
    val uniqueName: String?,
)

data class GroupUniqueNameStatusResponse(
    val totalGroupCount: Int,
    val missingUniqueNameCount: Int,
    val groups: List<GroupWithUniqueNameResponse>,
)

@RestController
@RequestMapping("/api/admin/groups")
class GroupController(
    private val msGraphGroup: MsGraphGroup,
) {
    @OnlyDevelopers
    @PostMapping("/full-sync")
    fun triggerGroupsFullSync(): ResponseEntity<TriggerResponse> {
        msGraphGroup.requestFullImport(false)

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
        msGraphGroup.requestFullImport(true)

        return ResponseEntity
            .status(HttpStatus.ACCEPTED)
            .body(TriggerResponse("Republish and full sync of groups triggered"))
    }

    @OnlyDevelopers
    @GetMapping("/objectid/{groupId}")
    fun getGroupInfo(
        @PathVariable groupId: String,
    ): ResponseEntity<*> {
        val group = msGraphGroup.getGroupInfo(groupId)

        return if (group == null) {
            ResponseEntity
                .status(HttpStatus.NOT_FOUND)
                .body(ErrorResponse("Group not found"))
        } else {
            ResponseEntity.ok(group)
        }
    }

    @OnlyDevelopers
    @GetMapping("/resourceid/{id}")
    fun getGroupInfoByResourceGroupId(
        @PathVariable id: Long,
    ): ResponseEntity<*> {
        val group = msGraphGroup.getGroupInfoByResourceGroupId(id)

        return if (group == null) {
            ResponseEntity
                .status(HttpStatus.NOT_FOUND)
                .body(ErrorResponseResourceId("Group not found in Entra"))
        } else {
            ResponseEntity.ok(group)
        }
    }

    @OnlyDevelopers
    @GetMapping("/unique-name-status")
    fun getGroupUniqueNameStatus(): ResponseEntity<GroupUniqueNameStatusResponse> {
        val groups =
            msGraphGroup.fetchGroupsForUniqueNameAudit().map { group ->
                GroupWithUniqueNameResponse(
                    id = group.id,
                    displayName = group.displayName,
                    uniqueName = group.uniqueName,
                )
            }

        return ResponseEntity.ok(
            GroupUniqueNameStatusResponse(
                totalGroupCount = groups.size,
                missingUniqueNameCount = groups.count { it.uniqueName.isNullOrBlank() },
                groups = groups,
            ),
        )
    }
}
