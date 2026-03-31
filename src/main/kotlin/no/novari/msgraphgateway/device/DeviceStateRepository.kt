@file:Suppress("ktlint:standard:no-wildcard-imports")

package no.novari.msgraphgateway.device

import java.time.Instant
import java.util.*

interface DeviceStateRepository {
    data class UpsertRow(
        val objectId: UUID,
        val checksum: ByteArray,
        val lastSeenAt: Instant,
    )

    fun findStaleObjectIds(cutoff: Instant): List<UUID>

    fun batchUpsertReturningChanged(rows: List<UpsertRow>): Set<UUID>

    fun deleteById(objectId: UUID)

    fun deleteByIdsReturningObjectIds(objectIds: Collection<UUID>): List<UUID>

    fun findStaleObjectIdsWithNotSeenCountGreaterThan(
        cutoff: Instant,
        minNotSeenCount: Int,
    ): List<UUID>

    fun incrementNotSeenCount(objectIds: Collection<UUID>)

    fun existsById(objectId: UUID): Boolean

    fun getCount(): Int
}
