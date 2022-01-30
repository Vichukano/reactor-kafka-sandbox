package ru.vichukano.reactor.kafka.sandbox.configuration;

import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.util.concurrent.atomic.AtomicInteger;


/**
 * Handle exception in reactive consumer stream with reactor kafka
 */
@Slf4j
@Configuration
public class ServiceEntryPoint {
    private final AtomicInteger counter = new AtomicInteger(0);

    @Bean
    public Disposable senderFlow(KafkaSender<Integer, String> kafkaSender) {
        Flux<SenderRecord<Integer, String, Integer>> senderFlux = Flux.just("one", "two", "three", "four", "five")
                .map(e -> SenderRecord.create(
                        KafkaConfig.TOPIC, 1, System.currentTimeMillis(), counter.incrementAndGet(), e, counter.get()
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

    /**
     * Need to ack record which produces exception.
     * After receive Mono.empty() signal consumer flux stops and recreate with repeat() command.
     * If poison record does not committed, then flux will be stopped and recreate in infinite loop
     */
    @Bean
    public Disposable consumerFlux(KafkaReceiver<Integer, String> kafkaReceiver) {
        log.info("Start kafka receiver");
        return kafkaReceiver.receive()
                .doOnNext(r -> {
                    log.info("Received: offset: {}, key: {}, value: {}", r.receiverOffset().offset(), r.key(), r.value());
                    if ("three".equals(r.value())) throw new ReactiveExceptionWrapper(r, new RuntimeException("BOOM"));
                })
                .doOnNext(r -> r.receiverOffset().acknowledge())//Only ack records manually
                .retry(3)//We can retry in some conditions
                .onErrorResume(e -> {
                    log.error("Got error", e);
                    if (e instanceof ReactiveExceptionWrapper) {
                        var record = ((ReactiveExceptionWrapper) e).getRecord();
                        record.receiverOffset().acknowledge();
                    }
                    return Mono.empty();
                })
                .repeat()//Recreate consumer if error or Mono.empty
                .subscribe();
    }

    @Value
    @EqualsAndHashCode(callSuper = true)
    private static class ReactiveExceptionWrapper extends RuntimeException {
        ReceiverRecord<Integer, String> record;
        Throwable exception;
    }
}
