package com.tradestream.ingestor.service;

import com.tradestream.ingestor.model.MarketData;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaMarketDataDataProducer implements MarketDataProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaMarketDataDataProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(MarketData data) {
        kafkaTemplate.send("market-data", data.getSymbol(), data);

        System.out.println("Kafka Implementation ile gönderildi: " + data.getSymbol());
    }
}
