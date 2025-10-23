package com.app.products.service.impl;

import com.app.products.event.ProductCreatedEvent;
import com.app.products.model.CreateProductModel;
import com.app.products.service.ProductService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class ProductServiceImpl implements ProductService {

    KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;
    private final Logger logger  = LoggerFactory.getLogger(this.getClass());

    public ProductServiceImpl(KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public String createProduct(CreateProductModel productRestModel) throws Exception {

        String productId = UUID.randomUUID().toString();

        // TODO: Persist Product Details into database table before publishing an Event

        ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent(productId,
                productRestModel.getTitle(), productRestModel.getPrice(),
                productRestModel.getQuantity());

        logger.info("Before publishing a ProductCreatedEvent");

        SendResult<String, ProductCreatedEvent> result =
                kafkaTemplate.send("product-created-events-topic",productId, productCreatedEvent).get();

        logger.info("Partition: " + result.getRecordMetadata().partition());
        logger.info("Topic: " + result.getRecordMetadata().topic());
        logger.info("Offset: " + result.getRecordMetadata().offset());

        logger.info("***** Returning product id");

        return productId;
    }

}

