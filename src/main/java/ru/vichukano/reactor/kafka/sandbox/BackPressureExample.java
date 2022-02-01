package ru.vichukano.reactor.kafka.sandbox;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import ru.vichukano.reactor.kafka.sandbox.configuration.SenderFlowConfig;

import java.time.Duration;
import java.util.Collections;

@Slf4j
@Configuration
public class BackPressureExample {

    /**
     * Concurrently process 2 records with delay of 5 seconds
     */
    @Bean
    public Disposable handleExceptionFlow(ReceiverOptions<Integer, String> receiverOptions) {
        log.info("Start kafka receiver");
        return KafkaReceiver.create(receiverOptions
                        .commitBatchSize(1)//Commit every record
                        .commitInterval(Duration.ZERO)
                        .subscription(Collections.singletonList(SenderFlowConfig.TOPIC)))
                .receive()
                .flatMap(r -> Mono.delay(Duration.ofSeconds(5))
                        .doOnNext(delay -> {
                            log.info("Received: offset: {}, key: {}, value: {}", r.receiverOffset().offset(), r.key(), r.value());
                            r.receiverOffset().acknowledge();
                        })
                        .then(), 2)
                .repeat()
                .subscribe();
    }

}
