package no.novari.msgraphgateway.azure

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException

// FULLY AI generated so will need to be checked and updated,
// for now just using for testing purposes
@Service
class ChecksumService {

    private val mapper: ObjectMapper = ObjectMapper()
        // Stable output:
        .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
        .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
        // Avoid nulls affecting output (optional; pick one rule and keep it forever)
        .setSerializationInclusion(JsonInclude.Include.NON_NULL)

    private val sha256: MessageDigest = try {
        MessageDigest.getInstance("SHA-256")
    } catch (e: NoSuchAlgorithmException) {
        throw IllegalStateException("SHA-256 not available", e)
    }

    fun checksum(dto: Any?): ByteArray {
        if (dto == null) {
            return ByteArray(0)
        }

        val canonicalBytes = toCanonicalJsonBytes(dto)
        return sha256Digest(canonicalBytes)
    }

    private fun toCanonicalJsonBytes(dto: Any): ByteArray {
        return try {
            // Canonical JSON (stable ordering)
            val json = mapper.writeValueAsString(dto)
            json.toByteArray(StandardCharsets.UTF_8)
        } catch (e: JsonProcessingException) {
            // Fallback: should not happen for a DTO; but keep behavior deterministic
            val fallback = dto::class.java.name
            log.warn(
                "Failed to serialize {} for checksum, using fallback",
                dto::class.java.simpleName,
                e
            )
            fallback.toByteArray(StandardCharsets.UTF_8)
        }
    }

    private fun sha256Digest(input: ByteArray): ByteArray {
        // MessageDigest is not thread-safe if reused; synchronize or create per call.
        // Here we synchronize to keep it simple.
        synchronized(sha256) {
            sha256.reset()
            return sha256.digest(input)
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(ChecksumService::class.java)
    }
}
