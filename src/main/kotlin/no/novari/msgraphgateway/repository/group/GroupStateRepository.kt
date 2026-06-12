package no.novari.msgraphgateway.repository.group

import no.novari.msgraphgateway.services.Checksum
import java.time.Instant
import java.util.UUID

interface GroupStateRepository {
    data class UpsertRow(
        val objectId: UUID,
        val checksum: Checksum,
        val lastSeenAt: Instant,
    )

    fun findStaleObjectIds(cutoff: Instant): List<UUID>

    fun batchUpsertReturningChanged(rows: List<UpsertRow>): Set<UUID>

    fun batchUpsert(rows: List<UpsertRow>)

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
