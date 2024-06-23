package com.verygoodbank.tes.web.enricher;

import java.io.InputStream;
import java.io.OutputStream;

public interface Enricher {
    void enrich(OutputStream outputStream, InputStream inputStream);
}
