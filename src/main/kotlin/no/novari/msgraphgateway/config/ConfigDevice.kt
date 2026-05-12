package no.novari.msgraphgateway.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component
import java.util.LinkedHashSet

@Component
@ConfigurationProperties(prefix = "ms-graph.device")
class ConfigDevice(
    var deviceIdAttribute: String? = null,
    var devicePagingSize: Int? = null,
    var staleAfterDays: Int = 7,
    var acceptedDeviationPercent: Int? = null,
    var minNotSeenCount: Int = 7,
    var attributes: List<String> = DEFAULT_DEVICE_ATTRIBUTES,
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

    companion object {
        private val DEFAULT_DEVICE_ATTRIBUTES =
            listOf(
                "id",
                "deviceId",
                "displayName",
                "accountEnabled",
                "operatingSystem",
                "operatingSystemVersion",
                "trustType",
                "profileType",
                "isCompliant",
                "isManaged",
                "approximateLastSignInDateTime",
                "registrationDateTime",
            )
    }
}
