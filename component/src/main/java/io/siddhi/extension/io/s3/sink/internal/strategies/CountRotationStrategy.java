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

package io.siddhi.extension.io.s3.sink.internal.strategies;

import com.amazonaws.services.s3.AmazonS3;
import io.siddhi.extension.io.s3.sink.internal.RotationStrategy;
import io.siddhi.extension.io.s3.sink.internal.beans.EventObject;
import io.siddhi.extension.io.s3.sink.internal.beans.SinkConfig;
import io.siddhi.extension.io.s3.sink.internal.publisher.EventPublisherThreadPoolExecutor;
import io.siddhi.extension.io.s3.sink.internal.publisher.PublisherTask;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class CountRotationStrategy implements RotationStrategy {
    private AmazonS3 client;
    private String bucketName;
    private int flushSize;

    private Map<String, Integer> eventOffsetMap = new HashMap<>();
    private Map<String, EventObject> eventObjectMap = new HashMap<>();

    private BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();

    public CountRotationStrategy(SinkConfig config, AmazonS3 client) {
        this.flushSize = config.getFlushSize();
        this.bucketName = config.getBucketName();
        this.client = client;
    }

    @Override
    public void queueEvent(String objectPath, Object event) {
        EventObject eventObject = getOrCreateEventObject(objectPath);
        eventObject.addEvent(event);
        incrementEventOffset(objectPath);

        if (eventObject.getEventCount() % flushSize == 0) {
            System.out.println(">>>>>>>>>>> Queuing the event object with " + eventObject.getEventCount()
                    + " events. Offset: " + eventObject.getOffset());

            taskQueue.add(new PublisherTask(eventObject, client));
            eventObjectMap.replace(objectPath, new EventObject(bucketName, objectPath, getEventOffset(objectPath)));

            System.out.println(">>>>>>>>>>> Queue contains " + taskQueue.size() + " tasks.");
        }
    }

    @Override
    public void publish(int numExecutors) {
        EventPublisherThreadPoolExecutor executor = new EventPublisherThreadPoolExecutor(
                10, 20, 5000, TimeUnit.MILLISECONDS, this.taskQueue);
        executor.prestartAllCoreThreads();
    }

    private EventObject getOrCreateEventObject(String objectPath ) {
        EventObject eventObject = eventObjectMap.get(objectPath);
        if (eventObject == null) {
            eventObject = new EventObject(bucketName, objectPath, getEventOffset(objectPath));
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
        int newOffset = getEventOffset(objectPath) + 1;
        eventOffsetMap.put(objectPath, newOffset);
    }
}
