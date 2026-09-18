package no.novari.msgraphgateway.controller

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping

class SyncControllerContractTest {
    @Test
    fun allObjectTypesExposeTheSameSyncTriggers() {
        val controllers =
            listOf(
                ControllerContract(UserController::class.java, "/api/admin/users", "User"),
                ControllerContract(DeviceController::class.java, "/api/admin/devices", "Device"),
                ControllerContract(GroupController::class.java, "/api/admin/groups", "Groups"),
            )

        controllers.forEach { contract ->
            val basePaths =
                contract.type
                    .getAnnotation(RequestMapping::class.java)
                    .value
                    .toSet()
            assertTrue(contract.basePath in basePaths, "${contract.type.simpleName} is missing ${contract.basePath}")

            assertPostMapping(contract, "trigger${contract.methodPrefix}FullSync", "/full-sync")
            assertPostMapping(contract, "trigger${contract.methodPrefix}DeltaSync", "/delta-sync")
            assertPostMapping(contract, "triggerRepublishAndFullSync", "/republish-and-full-sync")
        }
    }

    private fun assertPostMapping(
        contract: ControllerContract,
        methodName: String,
        expectedPath: String,
    ) {
        val paths =
            contract.type
                .getDeclaredMethod(methodName)
                .getAnnotation(PostMapping::class.java)
                .value
                .toSet()

        assertTrue(expectedPath in paths, "${contract.type.simpleName} is missing $expectedPath")
    }

    private data class ControllerContract(
        val type: Class<*>,
        val basePath: String,
        val methodPrefix: String,
    )
}
