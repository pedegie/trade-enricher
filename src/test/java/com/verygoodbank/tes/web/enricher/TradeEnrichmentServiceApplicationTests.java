package com.verygoodbank.tes.web.enricher;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.verygoodbank.tes.web.enricher.concurrenct.BytesProductNameResolver;
import com.verygoodbank.tes.web.enricher.concurrenct.MultiThreadEnricherService;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.apache.tomcat.util.http.fileupload.ByteArrayOutputStream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.verygoodbank.tes.web.enricher.EnricherService.DATE_FORMAT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TradeEnrichmentServiceApplicationTests {

    @ParameterizedTest(name = "{0}")
    @MethodSource(value = "enrichers")
    void shouldEnrichTradeUsingProductName(EnricherCreator enricherCreator) {
        // GIVEN
        var productNames = Map.of(1L, "P1", 2L, "P2");
        var enricher = enricherCreator.create(productNames);

        var firstTradeRow = new String[]{"20100101", "1", "EUR", "10.0"};
        var secondTradeRow = new String[]{"20100201", "2", "USD", "5.0"};
        var tradesInputStream = createFrom(firstTradeRow, secondTradeRow);

        // WHEN
        var enrichedOutputStream = new ByteArrayOutputStream();
        enricher.enrich(enrichedOutputStream, tradesInputStream);
        var enriched = fromOutputStream(enrichedOutputStream);

        // THEN
        assertEquals(2, enriched.size());

        var p1 = findWithName(enriched, "P1");
        assertEquals(LocalDate.of(2010, 1, 1), p1.getDate());
        assertEquals("EUR", p1.getCurrency());
        assertThat(new BigDecimal("10")).isEqualByComparingTo(p1.getPrice());


        var p2 = findWithName(enriched, "P2");
        assertEquals(LocalDate.of(2010, 2, 1), p2.getDate());
        assertEquals("USD", p2.getCurrency());
        assertThat(new BigDecimal("5")).isEqualByComparingTo(p2.getPrice());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource(value = "enrichers")
    void shouldEnrichTradeUsingDefaultProductNameWhenCannotFindMapping(EnricherCreator enricherCreator) {
        // GIVEN
        var enricher = enricherCreator.create(Collections.emptyMap());
        var tradesInputStream = createFrom(new String[]{"20100101", "1", "EUR", "10.0"});

        // WHEN
        var enrichedOutputStream = new ByteArrayOutputStream();
        enricher.enrich(enrichedOutputStream, tradesInputStream);
        var enriched = fromOutputStream(enrichedOutputStream);

        // THEN
        assertEquals(EnricherService.DEFAULT_PRODUCT_NAME, enriched.get(0).getProductName());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource(value = "enrichers")
    void shouldDiscardRowOnInvalidDateFormat(EnricherCreator enricherCreator) {
        // GIVEN
        var enricher = enricherCreator.create(Map.of(1L, "P1"));
        var invalidDates = new String[]{"2010-10-01", "", "xxx"};

        Arrays.stream(invalidDates).forEach(invalidDateFormat -> {

            String[] invalidDateTrade = {invalidDateFormat, "1", "EUR", "10.0"};
            String[] validDateTrade = {"20101001", "1", "EUR", "10.0"};
            var tradesInputStream = createFrom(invalidDateTrade, validDateTrade);

            // WHEN
            var enrichedOutputStream = new ByteArrayOutputStream();
            enricher.enrich(enrichedOutputStream, tradesInputStream);
            var enriched = fromOutputStream(enrichedOutputStream);

            // THEN
            assertEquals(1, enriched.size());
            assertEquals("P1", enriched.get(0).getProductName());
        });
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource(value = "enrichers")
    void shouldSkipRowsWithAnInvalidNumberOfColumns(EnricherCreator enricherCreator) {
        // GIVEN
        var enricher = enricherCreator.create(Map.of(1L, "P1"));
        String[] threeColumnsRow = {"1", "EUR", "10.0"};
        String[] zeroColumnsRow = {""};
        String[] fiveColumnsRow = {"20101001", "1", "EUR", "10.0", "X"};
        String[] validTrade = {"20101001", "1", "EUR", "10.0"};
        var tradesInputStream = createFrom(threeColumnsRow, zeroColumnsRow, fiveColumnsRow, validTrade);

        // WHEN
        var enrichedOutputStream = new ByteArrayOutputStream();
        enricher.enrich(enrichedOutputStream, tradesInputStream);
        var enriched = fromOutputStream(enrichedOutputStream);

        // THEN
        assertEquals(1, enriched.size());
        assertEquals("P1", enriched.get(0).getProductName());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource(value = "enrichers")
    void shouldReturnNothingIfTheInputIsEmpty(EnricherCreator enricherCreator) {
        // GIVEN
        var enricher = enricherCreator.create(Collections.emptyMap());

        // WHEN
        var enrichedOutputStream = new ByteArrayOutputStream();
        enricher.enrich(enrichedOutputStream, new ByteArrayInputStream(new byte[]{}));
        var enriched = fromOutputStream(enrichedOutputStream);

        // THEN
        assertTrue(enriched.isEmpty());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource(value = "enrichers")
    @SneakyThrows
    void shouldThrowAnExceptionWhenInputStreamIsNull(EnricherCreator enricherCreator) {
        // GIVEN
        var enricher = enricherCreator.create(Collections.emptyMap());
        var enrichedOutputStream = new ByteArrayOutputStream();

        // EXPECT
        assertThrows(IllegalArgumentException.class, () -> enricher.enrich(enrichedOutputStream, null));

        enrichedOutputStream.close();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource(value = "enrichers")
    @SneakyThrows
    void shouldThrowAnExceptionWhenOutputStreamIsNull(EnricherCreator enricherCreator) {
        // GIVEN
        var enricher = enricherCreator.create(Collections.emptyMap());
        var inputStream = new ByteArrayInputStream(new byte[]{});

        // EXPECT
        assertThrows(IllegalArgumentException.class, () -> enricher.enrich(null, inputStream));

        inputStream.close();
    }

    private EnrichedTrade findWithName(List<EnrichedTrade> trades, String name) {
        return trades.stream()
                .filter(it -> it.getProductName().equals(name))
                .findAny()
                .orElseThrow();
    }

    @SneakyThrows
    private InputStream createFrom(String[]... rows) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CSVWriter writer = new CSVWriter(new OutputStreamWriter(out));
        writer.writeNext(CsvTradeReader.COLUMN_NAMES, false);
        Arrays.stream(rows).forEach(row -> writer.writeNext(row, false));
        writer.close();

        return new ByteArrayInputStream(out.toByteArray());
    }

    @SneakyThrows
    private List<EnrichedTrade> fromOutputStream(ByteArrayOutputStream outputStream) {
        CSVReader csvReader = new CSVReader(new InputStreamReader(new ByteArrayInputStream(outputStream.toByteArray())));
        csvReader.skip(CsvTradeReader.HEADER);
        List<EnrichedTrade> enriched = new ArrayList<>();
        csvReader.iterator().forEachRemaining(csvRecord -> {
            enriched.add(toEnriched(csvRecord));
        });
        return enriched;
    }

    private EnrichedTrade toEnriched(String[] csvRecord) {
        return EnrichedTrade.builder()
                .date(LocalDate.parse(csvRecord[0], DATE_FORMAT))
                .productName(csvRecord[1])
                .currency(csvRecord[2])
                .price(new BigDecimal(csvRecord[3]))
                .build();
    }

    private static Stream<Arguments> enrichers() {
        return Stream.of(
                Arguments.of(new NamedEnricher(mappings -> new EnricherService(new ProductNameResolver(mappings)), "SIMPLE_ENRICHER")),
                Arguments.of(new NamedEnricher(mappings -> new MultiThreadEnricherService(1, new BytesProductNameResolver(mappings)), "CONCURRENT_ENRICHER")));
    }

    @FunctionalInterface
    private interface EnricherCreator extends Function<Map<Long, String>, Enricher> {
        default Enricher create(Map<Long, String> mappings) {
            return apply(mappings);
        }
    }

    @RequiredArgsConstructor
    @FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
    private static class NamedEnricher implements EnricherCreator {
        EnricherCreator enricherCreator;
        String name;

        @Override
        public String toString() {
            return name;
        }

        @Override
        public Enricher apply(Map<Long, String> longStringMap) {
            return enricherCreator.apply(longStringMap);
        }
    }
}
