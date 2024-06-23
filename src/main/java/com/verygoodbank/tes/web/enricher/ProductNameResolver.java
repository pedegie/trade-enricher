package com.verygoodbank.tes.web.enricher;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Service
@Lazy
class ProductNameResolver {

    Map<Long, String> productNames;

    @Autowired
    public ProductNameResolver(@Value("${product-file-path}") String resourceFile) {
        this.productNames = initializeProductNameMappings(resourceFile);
    }

    ProductNameResolver(Map<Long, String> productNames) {
        this.productNames = productNames;
    }

    Optional<String> resolve(long productId) {
        return Optional.ofNullable(productNames.get(productId));
    }

    private Map<Long, String> initializeProductNameMappings(String resourceFile) {
        var mappingsList = ProductMappingsLoader.loadProductNameMappings(resourceFile);
        var mappings = new HashMap<Long, String>(mappingsList.size());
        mappingsList.forEach(row -> mappings.put(Long.parseLong(row[0]), row[1]));
        return mappings;
    }
}
