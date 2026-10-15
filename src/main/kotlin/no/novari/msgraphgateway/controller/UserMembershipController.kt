package no.novari.msgraphgateway.controller

import no.fintlabs.util.OnlyDevelopers
import no.novari.msgraphgateway.dto.UserMembershipResponse
import no.novari.msgraphgateway.dto.UserMembershipsStateResponse
import no.novari.msgraphgateway.services.member.UserMembershipService
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.server.ResponseStatusException
import java.time.OffsetDateTime

@RestController
@RequestMapping("/api/admin/user-memberships")
class UserMembershipController(
    private val membershipService: UserMembershipService,
) {
    @OnlyDevelopers
    @GetMapping("/without-desired-state")
    fun getObservedMembershipsWithoutDesiredState(
        @RequestParam(defaultValue = "0") page: Int,
        @RequestParam(defaultValue = "100") size: Int,
    ): ResponseEntity<UserMembershipsStateResponse> {
        if (page < 0 || size !in 1..MAX_PAGE_SIZE) {
            throw ResponseStatusException(
                HttpStatus.BAD_REQUEST,
                "page must be at least 0 and size must be between 1 and $MAX_PAGE_SIZE",
            )
        }

        val candidates =
            membershipService.findObservedMembershipsWithoutDesiredState(
                limit = size + 1,
                offset = page.toLong() * size,
            )
        val hasNextPage = candidates.size > size
        val memberships =
            candidates.take(size).map {
                UserMembershipResponse(
                    userRef = it.id.userRef,
                    groupRef = it.id.groupRef,
                    status = it.status,
                    desiredPresent = it.desiredPresent,
                    observedPresent = it.observedPresent,
                    createdAt = it.createdAt,
                    lastUpdatedAt = it.lastUpdatedAt,
                )
            }

        return ResponseEntity.ok(
            UserMembershipsStateResponse(
                count = memberships.size,
                page = page,
                size = size,
                hasNextPage = hasNextPage,
                memberships = memberships,
            ),
        )
    }

    @OnlyDevelopers
    @DeleteMapping("delete-all-memberships")
    fun deleteAllUserMemberships(): ResponseEntity<String> {
        val deletedCount = membershipService.deleteAllMemberships()
        return ResponseEntity.ok("Deleted all user memberships, total: $deletedCount")
    }

    @OnlyDevelopers
    @DeleteMapping("/delete-all-memberships/before")
    fun deleteUserMembershipsUpdatedBefore(
        @RequestParam before: OffsetDateTime,
    ): ResponseEntity<String> {
        val deletedCount = membershipService.deleteMembershipsUpdatedBefore(before)
        return ResponseEntity.ok("Deleted all user memberships before $before, total: $deletedCount")
    }

    companion object {
        private const val MAX_PAGE_SIZE = 999
    }
}
