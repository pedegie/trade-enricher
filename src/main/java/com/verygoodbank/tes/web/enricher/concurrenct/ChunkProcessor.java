package com.verygoodbank.tes.web.enricher.concurrenct;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.jctools.queues.SpmcArrayQueue;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.verygoodbank.tes.web.enricher.concurrenct.ChunkDispatcher.COLUMN_NAMES;
import static com.verygoodbank.tes.web.enricher.concurrenct.ChunkDispatcher.NEW_LINE;
import static com.verygoodbank.tes.web.enricher.concurrenct.MultiThreadEnricherService.BUFFER_SIZE;
import static com.verygoodbank.tes.web.enricher.concurrenct.MultiThreadEnricherService.POISON_PILL;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Slf4j
class ChunkProcessor implements Runnable {

    private static final ByteBuffer DEFAULT_PRODUCT_NAME = ByteBuffer.wrap("Missing Product Name".getBytes()).asReadOnlyBuffer();
    private static final int REQUIRED_VALUES = COLUMN_NAMES.length;
    SpmcArrayQueue<ByteBuffer> inputQueue;
    Sender sender;
    ByteBuffer outputBuffer = ByteBuffer.allocate(BUFFER_SIZE * 2);
    BytesProductNameResolver productNameResolver;
    AtomicBoolean running = new AtomicBoolean(true);

    public ChunkProcessor(SpmcArrayQueue<ByteBuffer> inputQueue, Sender sender, BytesProductNameResolver productNameResolver) {
        this.inputQueue = inputQueue;
        this.sender = sender;
        this.productNameResolver = productNameResolver;
    }

    @Override
    public void run() {
        try {
            while (true) {
                ByteBuffer processingBuffer;
                while ((processingBuffer = inputQueue.poll()) == null) {
                    Thread.onSpinWait();
                }

                if (processingBuffer == POISON_PILL) {
                    running.set(false);
                    break;
                }
                processBuffer(processingBuffer);
                outputBuffer.flip();
                sender.sendBuffer(outputBuffer);
                outputBuffer.clear();
            }
        } catch (Exception e) {
            log.error("", e);
        }
    }

    private void processBuffer(ByteBuffer buffer) {
        buffer.mark();
        int dateStartIndex = -1;
        int dateEndIndex = -1;

        int productIdStartIndex = -1;
        int productIdEndIndex = -1;

        var values = 0;
        var currentValueIndex = 0;
        while (buffer.hasRemaining()) {
            byte b = buffer.get();
            if (b == NEW_LINE) {
                if (values == REQUIRED_VALUES - 1) {
                    enrichTrade(dateStartIndex, dateEndIndex, productIdStartIndex, productIdEndIndex, buffer);
                }
                values = 0;
                currentValueIndex = buffer.position();
            }
            if (b == ',') {
                values++;
                if (values == 1) {
                    dateEndIndex = buffer.position() - 1;
                    dateStartIndex = currentValueIndex;
                } else if (values == 2) {
                    productIdEndIndex = buffer.position() - 1;
                    productIdStartIndex = currentValueIndex;
                }
                currentValueIndex = buffer.position();
            }
        }
    }

    private void enrichTrade(int dateStartIndex, int dateEndIndex, int productIdStartIndex, int productIdEndIndex, ByteBuffer buffer) {
        var currentPosition = buffer.position();
        var currentLimit = buffer.limit();

        buffer.position(dateStartIndex);
        buffer.limit(dateEndIndex);
        var dateValid = DateValidator.validate(buffer);
        if (!dateValid) {
            var arr = new byte[dateEndIndex - dateStartIndex];
            buffer.get(arr);
            log.error("Invalid date format: {}. Row discarded", new String(arr));
            buffer.limit(currentLimit);
            buffer.position(currentPosition);
            return;
        }

        for (int i = dateStartIndex; i < dateEndIndex; i++) {
            outputBuffer.put(buffer.get(i));
        }
        outputBuffer.put((byte) ',');
        buffer.limit(buffer.capacity());

        buffer.position(productIdStartIndex);
        buffer.limit(productIdEndIndex);

        var id = getProductId(productIdStartIndex, productIdEndIndex, buffer);
        var name = productNameResolver.resolve(id)
                .orElseGet(() -> logMissingProductAndReturnDefault(id));

        while (name.hasRemaining()) {
            outputBuffer.put(name.get());
        }

        buffer.position(productIdEndIndex);
        buffer.limit(currentPosition);
        buffer.position(currentPosition);
        for (int i = productIdEndIndex; i < currentPosition; i++) {
            outputBuffer.put(buffer.get(i));
        }

        buffer.limit(currentLimit);
        buffer.position(currentPosition);
    }

    public long getProductId(int productIdStartIndex, int productIdEndIndex, ByteBuffer buffer) {
        long value = 0;
        for (int i = productIdStartIndex; i < productIdEndIndex; i++) {
            char tmp = (char) buffer.get(i);
            int numeric = tmp - '0';
            value = value * 10 + numeric;
        }
        return value;
    }


    private static ByteBuffer logMissingProductAndReturnDefault(long productId) {
        log.warn("Missing product name mapping for product id: {}", productId);
        return DEFAULT_PRODUCT_NAME.slice();
    }

    public boolean isRunning() {
        return running.get();
    }
}
