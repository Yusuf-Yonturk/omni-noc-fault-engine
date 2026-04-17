package com.noc.correlation;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Correlation Engine — consumes alarm.normalized, groups alarms into incidents, publishes incident.updated. */
@SpringBootApplication
@EnableScheduling
public class CorrelationEngineApplication {

    public static void main(String[] args) {
        SpringApplication.run(CorrelationEngineApplication.class, args);
    }
}
