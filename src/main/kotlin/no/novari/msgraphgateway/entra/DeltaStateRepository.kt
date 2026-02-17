package no.novari.msgraphgateway.entra

import org.springframework.data.repository.CrudRepository

interface DeltaStateRepository : CrudRepository<DeltaState, String> {
    fun findDeltaStateByName(name: String): DeltaState?
}
