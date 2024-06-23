package com.verygoodbank.tes.web.enricher;

import com.opencsv.CSVReader;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.DefaultResourceLoader;

import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

import static com.verygoodbank.tes.web.enricher.CsvTradeReader.HEADER;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public final class ProductMappingsLoader {

    @SneakyThrows
    public static List<String[]> loadProductNameMappings(String resourceFile) {
        log.info("Loading product name mappings from: {}", resourceFile);
        Reader reader;
        if (resourceFile.startsWith("classpath:")) {
            var resourceLoader = new DefaultResourceLoader();
            reader = new InputStreamReader(resourceLoader.getResource(resourceFile).getInputStream());
        } else {
            reader = new FileReader(resourceFile);
        }
        try (var csvReader = new CSVReader(reader)) {
            csvReader.skip(HEADER);
            var mappingsList = csvReader.readAll();
            log.info("Loaded {} mappings", mappingsList.size());
            return mappingsList;
        }
    }
}
