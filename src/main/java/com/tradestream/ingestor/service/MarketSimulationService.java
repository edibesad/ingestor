package com.tradestream.ingestor.service;

import com.tradestream.ingestor.model.MarketData;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Random;

@Service
public class MarketSimulationService {

    private final MarketDataProducer producer;
    private final Random random = new Random();

    public MarketSimulationService(MarketDataProducer producer) {
        this.producer = producer;
    }

    @Scheduled(fixedRate = 1000)
    public void generateMarketData() {
        String[] symbols = {"BTC", "ETH", "SOL", "AVAX"};
        String symbol = symbols[random.nextInt(symbols.length)];

        double priceValue;
        if (random.nextInt(10) == 0) {
            priceValue = 100 + (5000 - 100) * random.nextDouble();
        } else {
            priceValue = 25000 + (60000 - 25000) * random.nextDouble();
        }

        BigDecimal price = BigDecimal.valueOf(priceValue);

        MarketData data = new MarketData(symbol, price, java.time.LocalDateTime.now().toString());

        producer.sendMessage(data);
    }
}
