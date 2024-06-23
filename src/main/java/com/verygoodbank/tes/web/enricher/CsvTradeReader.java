package com.verygoodbank.tes.web.enricher;

import com.opencsv.CSVReader;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.stream.Streams;

import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.verygoodbank.tes.web.enricher.EnricherService.DATE_FORMAT;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Slf4j
class CsvTradeReader implements Closeable {

    static final int HEADER = 1;
    static final String[] COLUMN_NAMES = {"date", "product_id", "currency", "price"};

    CSVReader csvReader;

    public CsvTradeReader(Reader bufferedReader) {
        this.csvReader = new CSVReader(bufferedReader);
    }

    @SneakyThrows
    public Stream<Trade> read() {
        csvReader.skip(HEADER);

        return Streams.of(csvReader.iterator())
                .filter(it -> it.length == COLUMN_NAMES.length)
                .map(this::toTrade)
                .filter(Objects::nonNull);
    }


    private Trade toTrade(String[] row) {
        var date = fetchData(row, 0, stringDate -> LocalDate.parse(stringDate, DATE_FORMAT));
        var productId = fetchData(row, 1, Long::parseLong);
        var currency = fetchData(row, 2, Objects::requireNonNull);
        var price = fetchData(row, 3, BigDecimal::new);

        if (date == null || productId == null || currency == null || price == null) {
            return null;
        }

        return Trade.builder()
                .date(date)
                .productId(productId)
                .currency(currency)
                .price(price)
                .build();
    }

    private <R> R fetchData(String[] row, int column, Function<String, R> fetcher) {
        try {
            return fetcher.apply(row[column]);
        } catch (Exception e) {
            log.error("Invalid {} format. Value: {}. Row discarded", COLUMN_NAMES[column], row[column]);
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        csvReader.close();
    }
}
