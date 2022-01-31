package ru.vichukano.reactor.kafka.sandbox;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import ru.vichukano.reactor.kafka.sandbox.configuration.SenderFlowConfig;

import java.time.Duration;
import java.util.Collections;


/**
 * Manual commit example
 */
@Slf4j
@Configuration
public class ManualCommitExample {

    /**
     * For manual commit we need to set commitBatchSize to 0 and commitInterval to Duration.ZERO.
     * Commit is async operation and return Mono<Void> and we need to block and wait commit execution.
     * Commit processing should be in separate threads
     */
    @Bean
    public Disposable manualCommitFlow(ReceiverOptions<Integer, String> receiverOptions) {
        log.info("Start kafka receiver");
        return KafkaReceiver.create(receiverOptions
                        .commitBatchSize(0)//Do not need autocommit acked records
                        .commitInterval(Duration.ZERO)
                        .subscription(Collections.singletonList(SenderFlowConfig.TOPIC)))
                .receive()
                .doOnNext(r -> log.info("Received: offset: {}, key: {}, value: {}", r.receiverOffset().offset(), r.key(), r.value()))
                .publishOn(Schedulers.boundedElastic())//Block commits should be in separate thread
                .doOnNext(r -> {
                    r.receiverOffset().commit().block(Duration.ofSeconds(10));
                    log.info("Committed offset: {}", r.receiverOffset().offset());
                })//Commit records manually
                .subscribe();
    }

}
