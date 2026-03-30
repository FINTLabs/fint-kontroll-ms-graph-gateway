package no.novari.msgraphgateway.entra

import com.microsoft.graph.models.Device

data class EntraDevice(
    var deviceId: String
    var displayName: String? = null
    var deviceType: String? = null
): Serializable {
    constructor(device: Device) : this(
        deviceId = device.id,
        displayName = device.displayName,
        deviceType = device.deviceType

    )

}