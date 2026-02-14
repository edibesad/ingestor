package com.tradestream.ingestor.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradestream.ingestor.model.MarketData;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class TradeAnalyticsStream {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SimpMessagingTemplate messagingTemplate;

    public TradeAnalyticsStream(SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, String> rawStream = streamsBuilder.stream("market-data",
                Consumed.with(Serdes.String(), Serdes.String()));

        rawStream
                .mapValues(value -> {
                    try { return objectMapper.readValue(value, MarketData.class); }
                    catch (Exception e) { return null; }
                })
                .filter((k, v) -> v != null)
                .groupBy((k, v) -> v.getSymbol(), Grouped.with(Serdes.String(), 
                        Serdes.serdeFrom(
                                (topic, data) -> {
                                    try {
                                        return objectMapper.writeValueAsBytes(data);
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                },
                                (topic, data) -> {
                                    try {
                                        return objectMapper.readValue(data, MarketData.class);
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                }
                        )))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(30)))
                .aggregate(
                        () -> new double[]{0.0, 0.0}, // [Toplam Fiyat, Adet]
                        (key, value, aggregate) -> {
                            aggregate[0] += value.getPrice().doubleValue(); // Fiyatı ekle
                            aggregate[1] += 1; // Sayacı artır
                            return aggregate;
                        },
                        Materialized.with(Serdes.String(), 
                                Serdes.serdeFrom(
                                        (topic, data) -> {
                                            try {
                                                return objectMapper.writeValueAsBytes(data);
                                            } catch (Exception e) {
                                                throw new RuntimeException(e);
                                            }
                                        },
                                        (topic, data) -> {
                                            try {
                                                return objectMapper.readValue(data, double[].class);
                                            } catch (Exception e) {
                                                throw new RuntimeException(e);
                                            }
                                        }
                                ))
                )
                .toStream()
                .foreach((window, aggregate) -> {
                    double average = aggregate[0] / aggregate[1];
                    String msg = String.format("📈 [%s] 30sn Ortalaması: %.2f$ (Örneklem: %.0f)",
                            window.key(), average, aggregate[1]);

                    System.out.println(msg);
                    messagingTemplate.convertAndSend("/topic/analytics", msg);
                });
    }
}
