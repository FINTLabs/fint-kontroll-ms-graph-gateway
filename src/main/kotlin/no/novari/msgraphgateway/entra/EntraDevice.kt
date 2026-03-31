@file:Suppress("ktlint:standard:no-wildcard-imports")

package no.novari.msgraphgateway.entra

import com.microsoft.graph.models.Device
import no.novari.msgraphgateway.config.ConfigDevice

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
    constructor(device: Device, configDevice: ConfigDevice) : this(
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
}
