package no.novari.msgraphgateway

import no.fintlabs.securityconfig.FintKontrollSecurityConfig
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.FilterType
import org.springframework.scheduling.annotation.EnableScheduling
import java.util.TimeZone

@EnableScheduling
@ConfigurationPropertiesScan
@SpringBootApplication(
    scanBasePackages = ["no.novari", "no.fintlabs"],
)
@ComponentScan(
    basePackages = ["no.novari", "no.fintlabs"],
    excludeFilters = [
        ComponentScan.Filter(
            type = FilterType.ASSIGNABLE_TYPE,
            classes = [FintKontrollSecurityConfig::class],
        ),
    ],
)
class Application

fun main(args: Array<String>) {
    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Oslo"))
    runApplication<Application>(*args)
}
