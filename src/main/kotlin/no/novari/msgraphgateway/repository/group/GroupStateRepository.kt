package no.novari.msgraphgateway.repository.group

import no.novari.msgraphgateway.services.Checksum
import java.time.Instant
import java.util.UUID

interface GroupStateRepository {
    data class UpsertRow(
        val objectId: UUID,
        val resourceGroupId: Long,
        val checksum: Checksum,
        val lastSeenAt: Instant,
    )

    data class DeletedRow(
        val objectId: UUID,
        val resourceGroupId: Long,
    )

    fun findStaleObjectIds(cutoff: Instant): List<UUID>

    fun batchUpsertReturningChanged(rows: List<UpsertRow>): Set<UUID>

    fun batchUpsert(rows: List<UpsertRow>)

    fun deleteById(objectId: UUID)

    fun deleteByIdsReturningRows(objectIds: Collection<UUID>): List<DeletedRow>

    fun findStaleObjectIdsWithNotSeenCountGreaterThan(
        cutoff: Instant,
        minNotSeenCount: Int,
    ): List<UUID>

    fun incrementNotSeenCount(objectIds: Collection<UUID>)

    fun existsById(objectId: UUID): Boolean

    fun findChecksumById(objectId: UUID): Checksum?

    fun findObjectIdByResourceGroupId(resourceGroupId: Long): UUID?

    fun findResourceGroupIdByObjectId(objectId: UUID): Long?

    fun getCount(): Int
}
