package kz.toganbayev.estore.transfers.service;

import kz.toganbayev.estore.transfers.persistence.TransferEntity;
import kz.toganbayev.estore.transfers.persistence.TransferRepository;
import kz.toganbayev.payments.ws.core.events.DepositRequestedEvent;
import kz.toganbayev.payments.ws.core.events.WithdrawalRequestedEvent;
import org.apache.kafka.common.Uuid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

import kz.toganbayev.estore.transfers.error.TransferServiceException;
import kz.toganbayev.estore.transfers.model.TransferRestModel;

@Service
public class TransferServiceImpl implements TransferService {
	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	private KafkaTemplate<String, Object> kafkaTemplate;
	private Environment environment;
	private RestTemplate restTemplate;
	private TransferRepository transferRepository;

	public TransferServiceImpl(KafkaTemplate<String, Object> kafkaTemplate, Environment environment,
			RestTemplate restTemplate, TransferRepository transferRepository) {
		this.kafkaTemplate = kafkaTemplate;
		this.environment = environment;
		this.restTemplate = restTemplate;
		this.transferRepository = transferRepository;
	}

	@Override
	@Transactional("transactionManager")
	public boolean transfer(TransferRestModel transferRestModel) {
		// Create withdrawal event (debit sender). Create deposit event (credit recipient)
		WithdrawalRequestedEvent withdrawalEvent = new WithdrawalRequestedEvent(transferRestModel.getSenderId(),
				transferRestModel.getRecepientId(), transferRestModel.getAmount());
		DepositRequestedEvent depositEvent = new DepositRequestedEvent(transferRestModel.getSenderId(),
				transferRestModel.getRecepientId(), transferRestModel.getAmount());

		try {
			TransferEntity transferEntity = new TransferEntity();
			BeanUtils.copyProperties(transferRestModel, transferEntity);
			transferEntity.setTransferId(Uuid.randomUuid().toString());
			transferRepository.save(transferEntity);
			// Step 1: Publish withdrawal event → withdrawal-service consumes + debits sender
			kafkaTemplate.send(environment.getProperty("withdraw-money-topic", "withdraw-money-topic"),
					withdrawalEvent);
			LOGGER.info("Sent event to withdrawal topic.");

			// Step 2: Call remote service (validation/verification step)
			callRemoteServce();

			// Step 3: Publish deposit event → deposit-service consumes + credits recipient
			kafkaTemplate.send(environment.getProperty("deposit-money-topic", "deposit-money-topic"), depositEvent);
			LOGGER.info("Sent event to deposit topic");

		} catch (Exception ex) {
			LOGGER.error(ex.getMessage(), ex);
			throw new TransferServiceException(ex);
		}

		return true;
	}

	private ResponseEntity<String> callRemoteServce() throws Exception {
		String requestUrl = "http://localhost:8082/response/200";
		ResponseEntity<String> response = restTemplate.exchange(requestUrl, HttpMethod.GET, null, String.class);

		if (response.getStatusCode().value() == HttpStatus.SERVICE_UNAVAILABLE.value()) {
			throw new Exception("Destination Microservice not availble");
		}

		if (response.getStatusCode().value() == HttpStatus.OK.value()) {
			LOGGER.info("Received response from mock service: " + response.getBody());
		}
		return response;
	}

}
