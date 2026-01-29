package no.novari.msgraphgateway.azure

import org.springframework.stereotype.Service

@Service
class DeltaLinkStore(private val deltaStateRepository: DeltaStateRepository) {

    fun find(name: String): String? {
        return deltaStateRepository.findDeltaStateByName(name)?.deltaLink
    }

    fun createOrUpdate(name: String, deltaLink: String) {
        val state = deltaStateRepository.findDeltaStateByName(name)
            ?.copy(
                deltaLink = deltaLink
            )
            ?: DeltaState(
                name = name,
                deltaLink = deltaLink
            )

        deltaStateRepository.save(state)
    }
}