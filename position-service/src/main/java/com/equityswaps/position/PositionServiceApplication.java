package com.equityswaps.position;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Position Service Application.
 * 
 * High-performance microservice for equity swaps position management
 * leveraging Java 23 Virtual Threads and reactive programming.
 */
@SpringBootApplication(scanBasePackages = {
    "com.equityswaps.position",
    "com.equityswaps.common"
})
@EnableR2dbcRepositories
@EnableKafka
@EnableAsync
@EnableScheduling
@ConfigurationPropertiesScan
public class PositionServiceApplication {

    public static void main(String[] args) {
        // Enable Virtual Threads for platform threads
        System.setProperty("spring.threads.virtual.enabled", "true");
        
        SpringApplication app = new SpringApplication(PositionServiceApplication.class);
        
        // Configure application for Java 23 features
        app.setAdditionalProfiles("virtual-threads");
        
        app.run(args);
    }
}