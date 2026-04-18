package com.noc.correlation.consumer;

import com.noc.correlation.event.AlarmNormalizedEvent;
import com.noc.correlation.service.CorrelationEngine;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/** Kafka listener for alarm.normalized topic. Manual ACK, concurrency=3, ordered per siteId partition. */
@Component
@RequiredArgsConstructor
@Slf4j
public class AlarmNormalizedEventConsumer {

    private final CorrelationEngine correlationEngine;

    @KafkaListener(
            topics       = "${noc.kafka.topics.alarm-normalized:alarm.normalized}",
            groupId      = "correlation-engine-group",
            concurrency  = "${noc.kafka.consumer.concurrency:3}",
            containerFactory = "alarmKafkaListenerContainerFactory"
    )
    public void consume(
            ConsumerRecord<String, AlarmNormalizedEvent> record,
            Acknowledgment acknowledgment,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        AlarmNormalizedEvent event = record.value();

        if (event == null) {
            log.warn("Received null event from partition={}, offset={} — skipping", partition, offset);
            acknowledgment.acknowledge();
            return;
        }

        log.debug("Consumed alarm: id={}, site={}, type={}, partition={}, offset={}",
                event.alarmId(), event.siteId(), event.alarmType(), partition, offset);

        try {
            correlationEngine.correlate(event);
            // Only commit after successful processing
            acknowledgment.acknowledge();

        } catch (Exception ex) {
            log.error("Failed to correlate alarm id={} from partition={} offset={}: {}",
                    event.alarmId(), partition, offset, ex.getMessage(), ex);
            // Do NOT acknowledge → will be reprocessed
            // In production: use a Dead Letter Topic (DLT) here
        }
    }
}
