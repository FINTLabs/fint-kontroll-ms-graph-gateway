package no.novari.msgraphgateway

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@EnableScheduling
@ConfigurationPropertiesScan
@SpringBootApplication(
    scanBasePackages = ["no.novari"],
)
class Application

fun main(args: Array<String>) {
    runApplication<Application>(*args)
}
