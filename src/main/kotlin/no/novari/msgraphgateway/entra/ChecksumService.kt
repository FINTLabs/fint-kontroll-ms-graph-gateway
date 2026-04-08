package no.novari.msgraphgateway.entra

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

@JvmInline
value class Checksum(
    val bytes: ByteArray,
)

@Service
class ChecksumService {
    private val mapper =
        JsonMapper
            .builder()
            .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
            .serializationInclusion(JsonInclude.Include.NON_NULL)
            .build()

    fun checksum(dto: Any?): Checksum {
        if (dto == null) return Checksum(ByteArray(0))

        val canonicalBytes = toCanonicalJsonBytes(dto)
        return Checksum(MessageDigest.getInstance("SHA-256").digest(canonicalBytes))
    }

    private fun toCanonicalJsonBytes(dto: Any): ByteArray =
        try {
            mapper.writeValueAsBytes(dto)
        } catch (e: JsonProcessingException) {
            log.warn(
                "Failed to serialize {} for checksum, using fallback",
                dto::class.java.simpleName,
                e,
            )
            dto::class.java.name.toByteArray(StandardCharsets.UTF_8)
        }

    companion object {
        private val log = LoggerFactory.getLogger(ChecksumService::class.java)
    }
}
