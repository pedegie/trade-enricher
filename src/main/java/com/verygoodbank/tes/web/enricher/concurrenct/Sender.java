package com.verygoodbank.tes.web.enricher.concurrenct;

import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Slf4j
class Sender {

    private static final String[] HEADER = {"date", "product_name", "currency", "price"};

    WritableByteChannel writableByteChannel;
    AtomicBoolean sending = new AtomicBoolean(false);

    public Sender(OutputStream outputStream) {
        this.writableByteChannel = Channels.newChannel(outputStream);
    }

    @SneakyThrows
    public void sendBuffer(ByteBuffer buffer) {
        lock();
        writableByteChannel.write(buffer);
        unlock();
    }

    private void unlock() {
        sending.set(false);
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private void lock() {
        while (!sending.compareAndSet(false, true)) ;
    }

    @SneakyThrows
    public void writeHeader() {
        var header = String.join(",", HEADER) + System.lineSeparator();
        writableByteChannel.write(ByteBuffer.wrap(header.getBytes()));
    }
}
