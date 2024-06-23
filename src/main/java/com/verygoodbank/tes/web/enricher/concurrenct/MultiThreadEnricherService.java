package com.verygoodbank.tes.web.enricher.concurrenct;

import com.verygoodbank.tes.web.enricher.Enricher;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.jctools.queues.SpmcArrayQueue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

@Service
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Slf4j
@Profile("concurrent")
@Lazy
public class MultiThreadEnricherService implements Enricher {

    static final ByteBuffer POISON_PILL = ByteBuffer.allocate(0);
    static final int BUFFER_SIZE = 8192;

    ExecutorService executorService;
    BytesProductNameResolver productNameResolver;
    int threads;

    public MultiThreadEnricherService(@Value("${processing-threads:-1}") int threads, BytesProductNameResolver productNameResolver) {
        this.threads = threads == -1 ? Runtime.getRuntime().availableProcessors() : threads;
        this.executorService = ThreadPools.chunkProcessorPool(this.threads);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> closeExecutor(executorService)));
        this.productNameResolver = productNameResolver;
    }

    @SneakyThrows
    @Override
    public void enrich(OutputStream outputStream, InputStream inputStream) {
        throwIfNull(outputStream, "Output");
        throwIfNull(inputStream, "Input");

        try (var source = new BufferedInputStream(inputStream, BUFFER_SIZE);
             var output = new BufferedOutputStream(outputStream, BUFFER_SIZE)) {
            var chunkQueue = new SpmcArrayQueue<ByteBuffer>(threads);

            var sender = new Sender(output);
            var chunksProcessors = initializeChunkProcessors(chunkQueue, sender);

            sender.writeHeader();
            var chunkDispatcher = new ChunkDispatcher(chunkQueue, source);

            chunkDispatcher.dispatch();

            closeChunkProcessor(chunkQueue, chunksProcessors);
        }
    }

    private static void throwIfNull(Closeable closeable, String stream) {
        if (closeable == null) {
            throw new IllegalArgumentException(stream + " stream cannot be null");
        }
    }

    private List<ChunkProcessor> initializeChunkProcessors(SpmcArrayQueue<ByteBuffer> chunkQueue, Sender sender) {
        List<ChunkProcessor> chunksProcessors = new ArrayList<>(threads);
        for (int i = 0; i < threads; i++) {
            var chunkProcessor = new ChunkProcessor(chunkQueue, sender, productNameResolver);
            var started = startProcessing(chunkProcessor);
            if (started) {
                chunksProcessors.add(chunkProcessor);
            } else if (startedAtLeastOneProcessor(i)) {
                break;
            } else {
                throw new SystemOverloadedException("Chunk pool overloaded, cannot create more chunk processors");
            }
        }
        return chunksProcessors;
    }

    private boolean startProcessing(ChunkProcessor chunkProcessor) {
        try {
            executorService.submit(chunkProcessor);
            return true;
        } catch (RejectedExecutionException e) {
            return false;
        }
    }

    private static boolean startedAtLeastOneProcessor(int i) {
        return i > 0;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private void closeChunkProcessor(SpmcArrayQueue<ByteBuffer> chunkQueue, List<ChunkProcessor> chunkProcessors) {
        for (int i = 0; i < chunkProcessors.size(); i++) {
            while (!chunkQueue.offer(POISON_PILL)) ;
        }
        for (ChunkProcessor chunkProcessor : chunkProcessors) {
            while (chunkProcessor.isRunning()) ;
        }
    }

    @SneakyThrows
    private void closeExecutor(ExecutorService executorService) {
        log.info("Closing thread pool");
        executorService.shutdownNow();
        var closed = executorService.awaitTermination(10, TimeUnit.SECONDS);
        if (closed) {
            log.info("Successfully closed thread pool");
        } else {
            log.info("Cannot close thread pool");
        }
    }
}
