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
import io.siddhi.extension.io.s3.sink.internal.strategies.CountRotationStrategy;
import io.siddhi.extension.io.s3.sink.internal.utils.S3Constants;
import io.siddhi.extension.io.s3.sink.internal.utils.ServiceClient;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class EventPublisher {
    private final ServiceClient client;
    RotationStrategy rotationStrategy;
    private OptionHolder optionHolder;
    private SinkConfig config;
    private BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();

    public EventPublisher(SinkConfig config, OptionHolder optionHolder) {
        this.optionHolder = optionHolder;
        this.config = config;
        this.client = new ServiceClient(config);
        this.rotationStrategy = new CountRotationStrategy(config, client, taskQueue);

        EventPublisherThreadPoolExecutor executor = new EventPublisherThreadPoolExecutor(
                10, 20, 5000, TimeUnit.MILLISECONDS, this.taskQueue);
        executor.prestartAllCoreThreads();
    }

    public void publish(Object payload, DynamicOptions dynamicOptions) {
        String objectPath = optionHolder.validateAndGetOption(S3Constants.OBJECT_PATH).getValue(dynamicOptions);
        rotationStrategy.queueEvent(objectPath, payload);
    }
}
