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

package io.siddhi.extension.io.s3.sink.internal.publisher;

import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.s3.sink.internal.RotationStrategy;
import io.siddhi.extension.io.s3.sink.internal.beans.SinkConfig;
import io.siddhi.extension.io.s3.sink.internal.strategies.countbased.CountBasedRotationStrategy;
import io.siddhi.extension.io.s3.sink.internal.strategies.interval.IntervalBasedRotationStrategy;
import io.siddhi.extension.io.s3.sink.internal.utils.S3Constants;
import io.siddhi.extension.io.s3.sink.internal.utils.ServiceClient;
import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class EventPublisher {

    private static final Logger logger = Logger.getLogger(EventPublisher.class);

    private ServiceClient client;
    private RotationStrategy rotationStrategy;
    private OptionHolder optionHolder;
    private SinkConfig config;
    private BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();
    private EventPublisherThreadPoolExecutor executor;

    public EventPublisher(SinkConfig config, OptionHolder optionHolder) {
        this.optionHolder = optionHolder;
        this.config = config;
        this.client = new ServiceClient(config);
        this.rotationStrategy = initRotationStrategy();

        logger.info(this.rotationStrategy.getName() + " rotation strategy has been selected.");

        this.executor = new EventPublisherThreadPoolExecutor(
                S3Constants.CORE_POOL_SIZE, S3Constants.MAX_POOL_SIZE, S3Constants.KEEP_ALIVE_TIME_MS,
                TimeUnit.MILLISECONDS, this.taskQueue);
        this.executor.prestartAllCoreThreads();
    }

    public void publish(Object payload, DynamicOptions dynamicOptions) {
        String objectPath = optionHolder.validateAndGetOption(S3Constants.OBJECT_PATH).getValue(dynamicOptions);
        logger.debug("Queuing the event for publishing: " + payload);
        rotationStrategy.queueEvent(objectPath, payload);
    }

    public void shutdown() {
        logger.info("Shutting down worker threads.");
        if (this.executor != null) {
            this.executor.shutdown();
        }
    }

    private RotationStrategy initRotationStrategy() {
//        if (this.config.getRotateIntetrvalMs() > -1) {
//            return new IntervalBasedRotationStrategy(config, client, taskQueue);
//        }

        // If the 'flush.size' is not set, make it 1 and initialize the count based strategy.
        if (this.config.getFlushSize() == -1) {
            this.config.setFlushSize(1);
        }
        return new CountBasedRotationStrategy(config, client, taskQueue);
    }
}
