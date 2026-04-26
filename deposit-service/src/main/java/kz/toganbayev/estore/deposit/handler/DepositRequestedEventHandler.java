package kz.toganbayev.estore.deposit.handler;

import kz.toganbayev.payments.ws.core.events.DepositRequestedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

// Consume deposit-money-topic. Process deposit txns from event stream
@Component
@KafkaListener(topics = "deposit-money-topic", containerFactory = "kafkaListenerContainerFactory")
public class DepositRequestedEventHandler {
	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	@KafkaHandler
	public void handle(@Payload DepositRequestedEvent depositRequestedEvent) {
		// Log deposit + process payment (sender → recipient)
		LOGGER.info("Received a new deposit event: {} ", depositRequestedEvent.getAmount());
	}
}
