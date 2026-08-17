package no.novari.msgraphgateway.repository

import no.novari.msgraphgateway.entra.DeltaState
import org.springframework.data.repository.CrudRepository

interface DeltaStateRepository : CrudRepository<DeltaState, String> {
    fun findDeltaStateByName(name: String): DeltaState?
}
