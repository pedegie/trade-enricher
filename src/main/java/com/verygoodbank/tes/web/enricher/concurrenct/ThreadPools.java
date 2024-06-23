package com.verygoodbank.tes.web.enricher.concurrenct;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class ThreadPools {

    public static ExecutorService chunkProcessorPool(int threads) {
        return new ThreadPoolExecutor(threads, threads,
                0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(200),
                new BasicThreadFactory.Builder().namingPattern("chunk-pool-%d").build(),
                new ThreadPoolExecutor.AbortPolicy());
    }
}
