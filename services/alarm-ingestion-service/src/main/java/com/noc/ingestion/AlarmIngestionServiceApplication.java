package com.noc.ingestion;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Alarm Ingestion Service — ingests, normalizes, and publishes alarms to Kafka. */
@SpringBootApplication
public class AlarmIngestionServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AlarmIngestionServiceApplication.class, args);
    }
}
