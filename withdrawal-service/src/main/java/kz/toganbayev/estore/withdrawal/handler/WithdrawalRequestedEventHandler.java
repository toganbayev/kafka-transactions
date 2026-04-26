package kz.toganbayev.estore.withdrawal.handler;

import kz.toganbayev.payments.ws.core.events.WithdrawalRequestedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

// Consume withdraw-money-topic. Process withdrawal txns from event stream
@Component
@KafkaListener(topics = "withdraw-money-topic", containerFactory = "kafkaListenerContainerFactory")
public class WithdrawalRequestedEventHandler {
    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    @KafkaHandler
    public void handle(@Payload WithdrawalRequestedEvent withdrawalRequestedEvent) {
        // Log withdrawal + debit user account
        LOGGER.info("Received a new withdrawal event: {} ", withdrawalRequestedEvent.getAmount());
    }
}
