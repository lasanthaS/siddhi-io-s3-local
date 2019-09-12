package io.siddhi.extension.io.s3.sink.internal.strategies.countbased;

import io.siddhi.extension.io.s3.sink.internal.beans.EventObject;
import io.siddhi.extension.io.s3.sink.internal.beans.SinkConfig;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class CountBasedEventObject extends EventObject {
    private static final Logger logger = Logger.getLogger(CountBasedEventObject.class);

    private int offset;

    public CountBasedEventObject(SinkConfig config, String objectPath, int offset) {
        super(config, objectPath);
        this.offset = offset;
    }

    @Override
    public InputStream serialize() {
        StringBuilder sb = new StringBuilder();
        this.events.forEach(e -> sb.append(e.toString()).append("\n"));
        return new ByteArrayInputStream(sb.toString().getBytes());
    }

    @Override
    public String toString() {
        return String.format("Event[offset=%d, path=%s, count=%s]", this.offset, this.objectPath, this.getEventCount());
    }

    @Override
    public String getObjectKey() {
        String extension = getExtension(this.config.getMapType());
        if (extension != null) {
            return String.format("%s-%d.json", this.config.getStreamId(), this.offset);
        }
        return String.format("%s-%d.txt", this.config.getStreamId(), this.offset);
    }

    public int getOffset() {
        return offset;
    }

    private String getExtension(String mapType) {
        if ("json".equalsIgnoreCase(mapType)) {
            return "json";
        }
        return null;
    }
}
