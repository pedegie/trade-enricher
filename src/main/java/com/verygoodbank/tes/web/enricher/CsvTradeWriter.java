package com.verygoodbank.tes.web.enricher;

import com.opencsv.CSVWriter;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;

import java.io.Closeable;
import java.io.IOException;
import java.io.Writer;

import static com.verygoodbank.tes.web.enricher.EnricherService.DATE_FORMAT;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
class CsvTradeWriter implements Closeable {

    private static final boolean APPLY_QUOTES = false;
    static final String[] HEADER = {"date", "product_name", "currency", "price"};

    CSVWriter csvWriter;

    public CsvTradeWriter(Writer bufferedWriter) {
        this.csvWriter = new CSVWriter(bufferedWriter);
        csvWriter.writeNext(HEADER);
    }

    @SneakyThrows
    public void write(EnrichedTrade toTrade) {
        csvWriter.writeNext(new String[]{toTrade.getDate().format(DATE_FORMAT), toTrade.getProductName(), toTrade.getCurrency(), toTrade.getPrice().toString()}, APPLY_QUOTES);
    }

    @Override
    public void close() throws IOException {
        csvWriter.close();
    }
}
