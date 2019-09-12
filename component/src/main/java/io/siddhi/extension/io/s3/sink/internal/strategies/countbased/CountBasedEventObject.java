package io.siddhi.extension.io.s3.sink.internal.strategies.countbased;

import io.siddhi.extension.io.s3.sink.internal.beans.EventObject;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class CountBasedEventObject extends EventObject {

    private static final Logger logger = Logger.getLogger(CountBasedEventObject.class);

    private int offset;

    public CountBasedEventObject(String bucketName, String objectPath, int offset, String contentType, String streamId, String mapType) {
        super(bucketName, objectPath, contentType, streamId, mapType);
        this.offset = offset;

        logger.info("Event object created with offset: " + offset);
    }

    @Override
    public InputStream serialize() {
        StringBuilder sb = new StringBuilder();
        this.getEvents().forEach(e -> sb.append(e.toString()).append("\n"));
        return new ByteArrayInputStream(sb.toString().getBytes());
    }

    @Override
    public String toString() {
        return String.format("Event[offset=%d, path=%s, count=%s]", this.getOffset(), this.getObjectPath(),
                this.getEventCount());
    }

    @Override
    public String getObjectKey() {
        String extension = this.getExtension(this.getMapType());
        if (extension != null) {
            return String.format("%s-%d.json", this.getStreamId(), this.getOffset());
        }
        return String.format("%s-%d.txt", this.getStreamId(), this.getOffset());
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    private String getExtension(String mapType) {
        if ("json".equalsIgnoreCase(mapType)) {
            return "json";
        }
        return null;
    }
}
