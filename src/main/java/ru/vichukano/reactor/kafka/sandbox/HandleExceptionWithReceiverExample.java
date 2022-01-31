package ru.vichukano.reactor.kafka.sandbox;

import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import ru.vichukano.reactor.kafka.sandbox.configuration.SenderFlowConfig;

import java.time.Duration;
import java.util.Collections;


/**
 * Handle exception in reactive consumer stream with reactor kafka
 */
@Slf4j
@Configuration
public class HandleExceptionWithReceiverExample {

    /**
     * Need to ack record which produces exception.
     * After receive Mono.empty() signal consumer flux stops and recreate with repeat() command.
     * If poison record does not committed, then flux will be stopped and recreate in infinite loop
     */
    @Bean
    public Disposable handleExceptionFlow(ReceiverOptions<Integer, String> receiverOptions) {
        log.info("Start kafka receiver");
        return KafkaReceiver.create(receiverOptions
                        .commitBatchSize(1)//Commit every record
                        .commitInterval(Duration.ZERO)
                        .subscription(Collections.singletonList(SenderFlowConfig.TOPIC)))
                .receive()
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
