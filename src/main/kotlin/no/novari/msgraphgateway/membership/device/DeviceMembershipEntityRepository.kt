package no.novari.msgraphgateway.membership.device

import jakarta.persistence.LockModeType
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Lock
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import java.util.UUID

@Repository
interface DeviceMembershipEntityRepository : JpaRepository<DeviceMembershipEntity, DeviceMembershipId> {
    fun findAllByIdDeviceRef(deviceRef: UUID): List<DeviceMembershipEntity>

    fun findAllByIdResourceRef(resourceRef: UUID): List<DeviceMembershipEntity>

    fun existsByIdDeviceRefAndIdResourceRef(
        deviceRef: UUID,
        resourceRef: UUID,
    ): Boolean

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select d from DeviceMembershipEntity d where d.id = :id")
    fun findByIdForUpdate(
        @Param("id") id: DeviceMembershipId,
    ): DeviceMembershipEntity?
}
