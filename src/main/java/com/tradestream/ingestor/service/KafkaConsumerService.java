package com.tradestream.ingestor.service;

import com.tradestream.ingestor.model.MarketData;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
public class KafkaConsumerService {

    private final BigDecimal BUY_THRESHOLD = new BigDecimal("20000");
    private final SimpMessagingTemplate messagingTemplate;

    public KafkaConsumerService(SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    @KafkaListener(topics = "market-data", groupId = "tradestream-group")
    public void consume(MarketData data) {

        messagingTemplate.convertAndSend("/topic/market", data);

        if (data.getPrice().compareTo(BUY_THRESHOLD) < 0) {
            messagingTemplate.convertAndSend("/topic/alerts", "🚨 ALARM: " + data.getSymbol() + " DÜŞTÜ!");
        }
    }
}
