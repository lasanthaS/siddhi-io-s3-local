/*
 *  Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package io.siddhi.extension.io.s3.sink.internal.strategies.countbased;

import io.siddhi.extension.io.s3.sink.internal.RotationStrategy;
import io.siddhi.extension.io.s3.sink.internal.beans.SinkConfig;
import io.siddhi.extension.io.s3.sink.internal.publisher.PublisherTask;
import io.siddhi.extension.io.s3.sink.internal.utils.ServiceClient;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class CountBasedRotationStrategy implements RotationStrategy {

    private static final Logger logger = Logger.getLogger(CountBasedRotationStrategy.class);

    private SinkConfig config;
    private ServiceClient client;
    private BlockingQueue<Runnable> taskQueue;

    private Map<String, Integer> eventOffsetMap = new HashMap<>();
    private Map<String, CountBasedEventObject> eventObjectMap = new HashMap<>();

    public CountBasedRotationStrategy(SinkConfig config, ServiceClient client, BlockingQueue<Runnable> taskQueue) {
        this.config = config;
        this.client = client;
        this.taskQueue = taskQueue;
    }

    @Override
    public void queueEvent(String objectPath, Object payload) {
        CountBasedEventObject eventObject = getOrCreateEventObject(objectPath);
        eventObject.addEvent(payload);
        incrementEventOffset(objectPath);

        if (eventObject.getEventCount() % this.config.getFlushSize() == 0) {
            logger.info("Queuing the event object with " + eventObject.getEventCount() + " events. Offset: " + eventObject.getOffset());

            taskQueue.add(new PublisherTask(eventObject, client));
            eventObjectMap.replace(objectPath,
                    new CountBasedEventObject(this.config, objectPath, getEventOffset(objectPath)));

            logger.info("Queue contains " + taskQueue.size() + " tasks.");
        }
    }

    @Override
    public String getName() {
        return "Count-based";
    }

    private CountBasedEventObject getOrCreateEventObject(String objectPath) {
        CountBasedEventObject eventObject = eventObjectMap.get(objectPath);
        if (eventObject == null) {
            eventObject = new CountBasedEventObject(this.config, objectPath, getEventOffset(objectPath));
            eventObjectMap.put(objectPath, eventObject);
        }
        return eventObject;
    }

    private int getEventOffset(String objectPath) {
        if (!eventOffsetMap.containsKey(objectPath)) {
            eventOffsetMap.put(objectPath, 0);
        }
        return eventOffsetMap.get(objectPath);
    }

    private void incrementEventOffset(String objectPath) {
        eventOffsetMap.put(objectPath, getEventOffset(objectPath) + 1);
    }
}
