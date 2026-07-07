package no.novari.msgraphgateway.entra.device

import com.microsoft.graph.models.Device

data class EntraDevice(
    val objectId: String?,
    val deviceId: String?,
    val displayName: String?,
    val accountEnabled: Boolean?,
    val operatingSystem: String?,
    val operatingSystemVersion: String?,
    val trustType: String?,
    val profileType: String?,
    val isManaged: Boolean?,
    val isCompliant: Boolean?,
    val approximateLastSignInDateTime: String?,
    val registrationDateTime: String?,
) {
    constructor(device: Device) : this(
        objectId = device.id,
        deviceId = device.deviceId,
        displayName = device.displayName,
        accountEnabled = device.accountEnabled,
        operatingSystem = device.operatingSystem,
        operatingSystemVersion = device.operatingSystemVersion,
        trustType = device.trustType,
        profileType = device.profileType,
        isManaged = device.isManaged,
        isCompliant = device.isCompliant,
        approximateLastSignInDateTime = device.approximateLastSignInDateTime?.toString(),
        registrationDateTime = device.registrationDateTime?.toString(),
    )

    companion object {
        val DEFAULT_DEVICE_ATTRIBUTES =
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
