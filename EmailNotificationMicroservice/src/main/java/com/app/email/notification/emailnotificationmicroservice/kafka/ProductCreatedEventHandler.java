package com.app.email.notification.emailnotificationmicroservice.kafka;

import com.app.common.kafka.ProductCreatedEvent;
import com.app.email.notification.emailnotificationmicroservice.entity.ProcessedEventEntity;
import com.app.email.notification.emailnotificationmicroservice.error.NotRetryableException;
import com.app.email.notification.emailnotificationmicroservice.repository.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@KafkaListener(topics="product-created-events-topic")
public class ProductCreatedEventHandler {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final ProcessedEventRepository processedEventRepository;

    public ProductCreatedEventHandler(ProcessedEventRepository processedEventRepository) {
        this.processedEventRepository = processedEventRepository;
    }

    @Transactional
    @KafkaHandler
    public void handle(@Payload ProductCreatedEvent productCreatedEvent, @Header("messageId") String messageId,
                       @Header(KafkaHeaders.RECEIVED_KEY) String messageKey) {
        logger.info("Received a new event: " + productCreatedEvent.getTitle() + " with productId: "
                + productCreatedEvent.getProductId());

        ProcessedEventEntity existingRecord = processedEventRepository.findByMessageId(messageId);

        if(existingRecord != null) {
            logger.info("Found a duplicate message id: {}", existingRecord.getMessageId());
            return;
        }

        try {
            processedEventRepository.save(new ProcessedEventEntity(messageId, productCreatedEvent.getProductId()));
        } catch (DataIntegrityViolationException ex) {
            throw new NotRetryableException(ex);
        }
    }
}
