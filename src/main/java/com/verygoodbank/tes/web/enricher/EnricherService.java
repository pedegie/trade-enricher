package com.verygoodbank.tes.web.enricher;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.time.format.DateTimeFormatter;

@Service
@Profile("serial")
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
@Slf4j
@Lazy
public class EnricherService implements Enricher {

    private static final int BUFFER_SIZE = 8192;
    static final String DEFAULT_PRODUCT_NAME = "Missing Product Name";
    public static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

    ProductNameResolver productNameResolver;

    @SneakyThrows
    @Override
    public void enrich(OutputStream outputStream, InputStream inputStream) {
        throwIfNull(outputStream, "Output");
        throwIfNull(inputStream, "Input");

        try (var reader = new CsvTradeReader(new BufferedReader(new InputStreamReader(inputStream), BUFFER_SIZE));
             var writer = new CsvTradeWriter(new BufferedWriter(new OutputStreamWriter(outputStream), BUFFER_SIZE))) {

            reader.read()
                    .map(this::enrich)
                    .forEach(writer::write);
        }
    }

    private static void throwIfNull(Closeable closeable, String stream) {
        if (closeable == null) {
            throw new IllegalArgumentException(stream + " stream cannot be null");
        }
    }

    private EnrichedTrade enrich(Trade trade) {
        var productName = productNameResolver.resolve(trade.getProductId())
                .orElseGet(() -> logMissingProductAndReturnDefault(trade.getProductId()));

        return EnrichedTrade.builder()
                .date(trade.getDate())
                .productName(productName)
                .currency(trade.getCurrency())
                .price(trade.getPrice())
                .build();
    }

    private static String logMissingProductAndReturnDefault(long productId) {
        log.warn("Missing product name mapping for product id: {}", productId);
        return DEFAULT_PRODUCT_NAME;
    }
}
