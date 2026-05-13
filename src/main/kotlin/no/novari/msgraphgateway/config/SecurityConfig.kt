package no.novari.msgraphgateway.config

import no.fintlabs.opa.KontrollAuthorizationManager
import no.fintlabs.securityconfig.CustomAccessDeniedHandler
import no.fintlabs.securityconfig.CustomAuthenticationEntryPoint
import no.fintlabs.util.JwtUserConverter
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity
import org.springframework.security.web.SecurityFilterChain

@Configuration
@EnableWebSecurity
class SecurityConfig(
    private val kontrollAuthorizationManager: KontrollAuthorizationManager,
    private val authenticationEntryPoint: CustomAuthenticationEntryPoint,
    private val accessDeniedHandler: CustomAccessDeniedHandler,
) {
    @Bean
    fun securityFilterChain(http: HttpSecurity): SecurityFilterChain =
        http
            .authorizeHttpRequests { auth ->
                auth
                    .requestMatchers("/actuator/**", "/favicon.ico", "/error")
                    .permitAll()
                    .anyRequest()
                    .access(kontrollAuthorizationManager)
            }.oauth2ResourceServer { resourceServer ->
                resourceServer
                    .jwt { jwt -> jwt.jwtAuthenticationConverter(JwtUserConverter()) }
                    .authenticationEntryPoint(authenticationEntryPoint)
            }.exceptionHandling { exceptionHandling ->
                exceptionHandling.accessDeniedHandler(accessDeniedHandler)
            }.build()
}
