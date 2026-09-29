package no.novari.msgraphgateway.controller

import no.fintlabs.util.OnlyDevelopers
import no.novari.msgraphgateway.services.member.UserMembershipService
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.time.OffsetDateTime

@RestController
@RequestMapping("/api/admin/user-memberships")
class UserMembershipController(
    private val membershipService: UserMembershipService,
) {
    @OnlyDevelopers
    @DeleteMapping
    fun deleteAllUserMemberships(): ResponseEntity<String> {
        val deletedCount = membershipService.deleteAllMemberships()
        return ResponseEntity.ok("Deleted all user memberships, count: $deletedCount")
    }

    @OnlyDevelopers
    @DeleteMapping("/before")
    fun deleteUserMembershipsUpdatedBefore(
        @RequestParam before: OffsetDateTime,
    ): ResponseEntity<String> {
        val deletedCount = membershipService.deleteMembershipsUpdatedBefore(before)
        return ResponseEntity.ok("Deleted all user memberships, count: $deletedCount")
    }
}
