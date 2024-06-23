package com.verygoodbank.tes.web.enricher.concurrenct;

public class SystemOverloadedException extends RuntimeException {

    public SystemOverloadedException(String message) {
        super(message);
    }
}
