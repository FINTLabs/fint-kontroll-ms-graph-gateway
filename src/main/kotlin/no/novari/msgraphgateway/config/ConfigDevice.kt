package no.novari.msgraphgateway.config

import jakarta.annotation.PostConstruct
import no.novari.msgraphgateway.entra.device.EntraDevice
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component
import java.util.LinkedHashSet
import kotlin.reflect.full.memberProperties

@Component
@ConfigurationProperties(prefix = "ms-graph.device")
class ConfigDevice(
    var devicePagingSize: Int? = null,
    var staleAfterDays: Int = 7,
    var acceptedDeviationPercent: Int? = null,
    var minNotSeenCount: Int = 7,
    var attributes: List<String> = EntraDevice.DEFAULT_DEVICE_ATTRIBUTES,
) {
    fun deviceAttributesDelta(): Array<String> {
        var wantsOnPremExtChild = false
        val cleaned = mutableListOf<String>()

        for (attribute in attributes) {
            val value = attribute.trim()
            if (value.isBlank()) continue

            if (value.startsWith("onPremisesExtensionAttributes.")) {
                wantsOnPremExtChild = true
                continue
            }

            if (value.contains(".")) continue

            cleaned.add(value)
        }

        if (wantsOnPremExtChild) {
            cleaned.add("onPremisesExtensionAttributes")
        }

        return LinkedHashSet(cleaned).toTypedArray()
    }

    @PostConstruct
    fun dumpConfig() {
        val log = LoggerFactory.getLogger(ConfigDevice::class.java)

        this::class
            .memberProperties
            .sortedBy { it.name }
            .forEach { property ->
                val value = property.getter.call(this)
                if (value != null) {
                    log.debug("{}={}", property.name, value)
                }
            }
    }
}
