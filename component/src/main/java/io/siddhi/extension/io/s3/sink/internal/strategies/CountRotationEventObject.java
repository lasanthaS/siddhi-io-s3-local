package io.siddhi.extension.io.s3.sink.internal.strategies;

import io.siddhi.extension.io.s3.sink.internal.beans.EventObject;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class CountRotationEventObject extends EventObject {

    public CountRotationEventObject(String bucketName, String objectPath, int offset, String contentType) {
        super(bucketName, objectPath, offset, contentType);
    }

    @Override
    public InputStream serialize() {
        StringBuilder sb = new StringBuilder();
        this.getEvents().forEach(e -> {
            sb.append(e.toString()).append("\n");
        });
        return new ByteArrayInputStream(sb.toString().getBytes());
    }
}
