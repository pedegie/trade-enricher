package com.verygoodbank.tes.web.enricher.concurrenct;

import com.verygoodbank.tes.web.enricher.ProductMappingsLoader;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Service
@Slf4j
@Lazy
public class BytesProductNameResolver {

    Map<Long, ByteBuffer> productNames;

    @Autowired
    public BytesProductNameResolver(@Value("${product-file-path}") String resourceFile) {
        this.productNames = initializeProductNameMappings(resourceFile);
    }

    public BytesProductNameResolver(Map<Long, String> productNames) {
        this.productNames = new HashMap<>(productNames.size());
        productNames.forEach((key, value) -> this.productNames.put(key, ByteBuffer.wrap(value.getBytes()).asReadOnlyBuffer()));
    }

    Optional<ByteBuffer> resolve(long productId) {
        return Optional.ofNullable(productNames.get(productId)).map(ByteBuffer::slice);
    }

    @SneakyThrows
    private static Map<Long, ByteBuffer> initializeProductNameMappings(String resourceFile) {
        var mappingsList = ProductMappingsLoader.loadProductNameMappings(resourceFile);
        var mappings = new HashMap<Long, ByteBuffer>(mappingsList.size());
        mappingsList.forEach(row -> mappings.put(Long.parseLong(row[0]), ByteBuffer.wrap(row[1].getBytes()).asReadOnlyBuffer()));
        return mappings;
    }
}
