package com.verygoodbank.tes.web.enricher.concurrenct;

import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.jctools.queues.SpmcArrayQueue;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import static com.verygoodbank.tes.web.enricher.concurrenct.MultiThreadEnricherService.BUFFER_SIZE;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Slf4j
class ChunkDispatcher {

    static final char NEW_LINE = '\n';
    static final String[] COLUMN_NAMES = {"date", "product_id", "currency", "price"};
    private static final ByteBuffer HEADER = ByteBuffer.wrap((String.join(",", COLUMN_NAMES) + NEW_LINE).getBytes());

    SpmcArrayQueue<ByteBuffer> queue;
    ReadableByteChannel readableByteChannel;

    public ChunkDispatcher(SpmcArrayQueue<ByteBuffer> queue, InputStream source) {
        this.readableByteChannel = Channels.newChannel(source);
        this.queue = queue;
        discardHeader();
    }

    @SneakyThrows
    private void discardHeader() {
        readableByteChannel.read(HEADER.slice());
    }

    @SneakyThrows
    public void dispatch() {
        try {
            var previousReadData = ByteBuffer.allocate(0);

            while (!Thread.interrupted()) {
                var processingBuffer = ByteBuffer.allocate(BUFFER_SIZE);
                processingBuffer.put(previousReadData);

                var read = readableByteChannel.read(processingBuffer);
                if (read == -1) {
                    break;
                }

                int position = moveBackwardToTheNewLineCharacter(processingBuffer);
                processingBuffer.position(position + 1);
                previousReadData = processingBuffer.slice();
                processingBuffer.limit(position + 1);
                processingBuffer.flip();

                while (!queue.offer(processingBuffer)) {
                    Thread.onSpinWait();
                }

            }
        } catch (Exception e) {
            log.info("Error during chunk read", e);
        }
    }

    private static int moveBackwardToTheNewLineCharacter(ByteBuffer processingBuffer) {
        int position = processingBuffer.position();
        while (position > 0 && processingBuffer.get(--position) != NEW_LINE) ;
        return position;
    }
}
