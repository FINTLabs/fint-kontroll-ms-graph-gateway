package no.novari.msgraphgateway.controller

import com.microsoft.graph.models.Group
import io.mockk.every
import io.mockk.mockk
import no.novari.msgraphgateway.group.MsGraphGroup
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class GroupControllerTest {
    private lateinit var msGraphGroup: MsGraphGroup
    private lateinit var controller: GroupController

    @BeforeEach
    fun setUp() {
        msGraphGroup = mockk()
        controller = GroupController(msGraphGroup)
    }

    @Test
    fun `unique name status counts null and blank values as missing`() {
        every { msGraphGroup.fetchGroupsForUniqueNameAudit() } returns
            listOf(
                group("configured", "Configured group", "resource-group-1"),
                group("null", "Null unique name", null),
                group("blank", "Blank unique name", "  "),
            )

        val response = controller.getGroupUniqueNameStatus()

        assertEquals(200, response.statusCode.value())
        assertEquals(3, response.body?.totalGroupCount)
        assertEquals(2, response.body?.missingUniqueNameCount)
        assertEquals(
            listOf("resource-group-1", null, "  "),
            response.body?.groups?.map { it.uniqueName },
        )
    }

    private fun group(
        id: String,
        displayName: String,
        uniqueName: String?,
    ): Group =
        Group().apply {
            this.id = id
            this.displayName = displayName
            this.uniqueName = uniqueName
        }
}
