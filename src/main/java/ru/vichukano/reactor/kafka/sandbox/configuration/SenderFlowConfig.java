package ru.vichukano.reactor.kafka.sandbox.configuration;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Configuration
public class SenderFlowConfig {
    public static String TOPIC = "reactor-kafka-sandbox-topic-1";
    private final AtomicInteger counter = new AtomicInteger(0);

    @Bean
    public Disposable senderFlow(KafkaSender<Integer, String> kafkaSender) {
        Flux<SenderRecord<Integer, String, Integer>> senderFlux = Flux.just("one", "two", "three", "four", "five")
                .map(e -> SenderRecord.create(
                        TOPIC, 1, System.currentTimeMillis(), counter.incrementAndGet(), e, counter.get()
                ));
        log.info("Start to send records");
        return kafkaSender.send(senderFlux)
                .doOnNext(r -> log.info("Send record: offset: {}, partition: {}, metadata: {}",
                        r.recordMetadata().offset(),
                        r.recordMetadata().partition(),
                        r.correlationMetadata()))
                .doOnComplete(() -> log.info("Sender Flux completed"))
                .subscribe();
    }
}
