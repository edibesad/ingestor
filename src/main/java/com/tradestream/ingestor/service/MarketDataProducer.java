package com.tradestream.ingestor.service;

import com.tradestream.ingestor.model.MarketData;

public interface MarketDataProducer {
    void sendMessage(MarketData data);
}
