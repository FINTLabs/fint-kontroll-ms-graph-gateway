package no.novari.msgraphgateway.device

import com.microsoft.graph.serviceclient.GraphServiceClient
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import no.novari.msgraphgateway.config.ConfigDevice
import no.novari.msgraphgateway.entra.DeltaLinkStore
import no.novari.msgraphgateway.repository.device.DeviceRepository
import no.novari.msgraphgateway.services.device.EntraDeviceSyncService
import org.junit.jupiter.api.Test

class MsGraphDeviceTest {
    @Test
    fun weeklyPublishDevicesRequestsFullImportWithRepublishAll() {
        val service =
            spyk(
                MsGraphDevice(
                    configDevice = mockk<ConfigDevice>(relaxed = true),
                    graphServiceClient = mockk<GraphServiceClient>(relaxed = true),
                    entraDeviceSyncService = mockk<EntraDeviceSyncService>(relaxed = true),
                    deltaLinkStore = mockk<DeltaLinkStore>(relaxed = true),
                    coreDeviceRepository = mockk<DeviceRepository>(relaxed = true),
                ),
            )
        every { service.requestFullImport(any()) } just Runs

        service.weeklyPublishDevices()

        verify(exactly = 1) { service.requestFullImport(true) }
    }
}
