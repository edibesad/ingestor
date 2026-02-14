package com.tradestream.ingestor.controller;

import com.tradestream.ingestor.model.MarketData;
import com.tradestream.ingestor.service.MarketDataProducer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@RestController
public class TestController {
    private final MarketDataProducer producer;

    public TestController(MarketDataProducer producer) {
        this.producer = producer;
    }

    @GetMapping("/api/send")
    public String sendData(@RequestParam String symbol, @RequestParam BigDecimal price) {
        MarketData data = new MarketData(symbol, price, LocalDateTime.now().toString());
        producer.sendMessage(data);
        return "İşlem Başarılı! Kafka'ya gönderildi: " + symbol;
    }
}
