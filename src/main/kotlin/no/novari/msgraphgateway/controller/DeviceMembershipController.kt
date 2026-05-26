package no.novari.msgraphgateway.controller

import no.fintlabs.util.OnlyDevelopers
import no.novari.msgraphgateway.membership.device.MembershipService
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.time.OffsetDateTime

@RestController
@RequestMapping("/api/admin/device-memberships")
class DeviceMembershipController(
    private val membershipService: MembershipService,
) {
    @OnlyDevelopers
    @DeleteMapping
    fun deleteAllDeviceMemberships(): ResponseEntity<String> {
        val deletedCount = membershipService.deleteAllMemberships()
        return ResponseEntity.ok("Deleted all device memberships, count: $deletedCount")
    }

    @OnlyDevelopers
    @DeleteMapping("/before")
    fun deleteDeviceMembershipsUpdatedBefore(
        @RequestParam before: OffsetDateTime,
    ): ResponseEntity<String> {
        val deletedCount = membershipService.deleteMembershipsUpdatedBefore(before)
        return ResponseEntity.ok("Deleted all device memberships, count: $deletedCount")
    }
}
