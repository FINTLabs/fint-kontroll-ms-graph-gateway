package no.novari.msgraphgateway.config

import com.azure.identity.ClientSecretCredentialBuilder
import com.microsoft.graph.serviceclient.GraphServiceClient
import com.microsoft.kiota.authentication.AzureIdentityAuthenticationProvider
import okhttp3.ConnectionPool
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.TimeUnit
import kotlin.emptyArray

@Configuration
@ConfigurationProperties(prefix = "ms-graph")
open class Config {
    var timeout: Long = 0L

    var credentials: Credentials = Credentials()

    class Credentials {
        var clientid: String? = null
        var clientsecret: String? = null
        var tenantguid: String? = null
        var entobjectid: String? = null
    }

    @Bean
    open fun graphServiceClient(): GraphServiceClient {
        log.debug("Starting PostConstruct of Graph Service Client")

        val scopes = arrayOf("https://graph.microsoft.com/.default")

        val cred =
            ClientSecretCredentialBuilder()
                .clientId(requireNotNull(credentials.clientid) { "ms-graph.credentials.clientid is required" })
                .tenantId(requireNotNull(credentials.tenantguid) { "ms-graph.credentials.tenantguid is required" })
                .clientSecret(
                    requireNotNull(credentials.clientsecret) { "ms-graph.credentials.clientsecret is required" },
                ).build()

        val dispatcher =
            Dispatcher().apply {
                maxRequests = 128
                maxRequestsPerHost = 64
            }

        val pool = ConnectionPool(100, 5, TimeUnit.MINUTES)

        val okHttpClient =
            OkHttpClient
                .Builder()
                .dispatcher(dispatcher)
                .connectionPool(pool)
                .callTimeout(timeout, TimeUnit.MINUTES)
                .connectTimeout(timeout, TimeUnit.MINUTES)
                .readTimeout(timeout, TimeUnit.MINUTES)
                .writeTimeout(timeout, TimeUnit.MINUTES)
                .retryOnConnectionFailure(true)
                .addInterceptor { chain ->
                    val request = chain.request()
                    val response = chain.proceed(request)

                    val requestClientId = request.header("client-request-id")
                    val responseClientId = response.header("client-request-id")
                    val responseRequestId = response.header("request-id")

                    if (responseRequestId.isNullOrBlank()) {
                        log.error("Missing request-id in Graph response (cannot correlate request!)")
                    }

                    if (requestClientId != null && responseClientId == null) {
                        log.debug(
                            "Graph did not return client-request-id. sent={} request-id={}",
                            requestClientId,
                            responseRequestId,
                        )
                    } else if (requestClientId != null && responseClientId != requestClientId) {
                        log.debug(
                            "client-request-id mismatch sent={} returned={} request-id={}",
                            requestClientId,
                            responseClientId,
                            responseRequestId,
                        )
                    }

                    val logMap =
                        mapOf(
                            "type" to "http_response",
                            "method" to request.method,
                            "url" to
                                request.url
                                    .newBuilder()
                                    .query(null)
                                    .build()
                                    .toString(),
                            "client-request-id" to requestClientId,
                            "returned-client-request-id" to responseClientId,
                            "request-id" to responseRequestId,
                            "date" to response.header("Date"),
                            "status" to response.code,
                        )

                    log.debug(logMap.toString())

                    response
                }.build()

        return GraphServiceClient(
            AzureIdentityAuthenticationProvider(cred, emptyArray<String>(), *scopes),
            okHttpClient,
        )
    }

    companion object {
        private val log = LoggerFactory.getLogger(Config::class.java)
    }
}
