package no.novari.msgraphgateway.azure

import org.springframework.data.repository.CrudRepository

interface DeltaStateRepository: CrudRepository<DeltaState, String>
{
    fun findDeltaStateByName(name: String): DeltaState?
}