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
@ConfigurationProperties(prefix = "azure")
open class Config {

    var timeout: Int = 0

    var credentials: Credentials = Credentials()

    @ConfigurationProperties(prefix = "azure.credentials")
    class Credentials {
        var clientid: String? = null
        var clientsecret: String? = null
        var tenantguid: String? = null
        var entobjectid: String? = null
    }

    @Bean
    open fun graphServiceClient(): GraphServiceClient {
        log.debug("Starting PostConstruct of GraphServiceClient")

        val scopes = arrayOf("https://graph.microsoft.com/.default")

        val cred = ClientSecretCredentialBuilder()
            .clientId(requireNotNull(credentials.clientid) { "azure.credentials.clientid is required" })
            .tenantId(requireNotNull(credentials.tenantguid) { "azure.credentials.tenantguid is required" })
            .clientSecret(requireNotNull(credentials.clientsecret) { "azure.credentials.clientsecret is required" })
            .build()

        val dispatcher = Dispatcher().apply {
            maxRequests = 128
            maxRequestsPerHost = 64
        }

        val pool = ConnectionPool(100, 5, TimeUnit.MINUTES)

        val okHttpClient = OkHttpClient.Builder()
            .dispatcher(dispatcher)
            .connectionPool(pool)
            .callTimeout(timeout.toLong(), TimeUnit.MINUTES)
            .connectTimeout(timeout.toLong(), TimeUnit.MINUTES)
            .readTimeout(timeout.toLong(), TimeUnit.MINUTES)
            .writeTimeout(timeout.toLong(), TimeUnit.MINUTES)
            .retryOnConnectionFailure(true)
            .build()

        return GraphServiceClient(
            AzureIdentityAuthenticationProvider(cred,  emptyArray<String>(), *scopes),
            okHttpClient
        )
    }

    companion object {
        private val log = LoggerFactory.getLogger(Config::class.java)
    }
}
