package com.verygoodbank.tes.web.enricher;

import com.verygoodbank.tes.web.enricher.concurrenct.SystemOverloadedException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@ControllerAdvice
@Slf4j
class EnricherExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(value = Exception.class)
    public ResponseEntity<Void> handleException(Exception e) {
        log.error("Unexpected error occurred", e);
        return ResponseEntity.internalServerError().build();
    }

    @ExceptionHandler(value = IllegalArgumentException.class)
    public ResponseEntity<String> handleIllegalException(IllegalArgumentException exc) {
        log.debug(exc.getMessage());
        return ResponseEntity.badRequest().body(exc.getMessage());
    }

    @ExceptionHandler(value = SystemOverloadedException.class)
    public ResponseEntity<String> handleIllegalException(SystemOverloadedException exc) {
        log.warn(exc.getMessage());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
    }
}
