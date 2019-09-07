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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import io.siddhi.extension.io.s3.sink.internal.beans.EventObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;

public class PublisherTask implements Runnable {

    private AmazonS3 client;
    private EventObject eventObject;

    public PublisherTask(EventObject eventObject, AmazonS3 client) {
        this.eventObject = eventObject;
        this.client = client;
    }

    @Override
    public void run() {
        System.out.println(">>>>>>>>>>> Publishing event " + eventObject);
        String content = buildObjectContent();
        InputStream inputStream = new ByteArrayInputStream(content.getBytes());

        ObjectMetadata metadata = new ObjectMetadata();
        try {
            metadata.setContentLength(inputStream.available());
        } catch (IOException e) {
            // Ignore setting content length
        }
        metadata.setContentType("application/octet-stream");

        PutObjectRequest request = new PutObjectRequest(eventObject.getBucketName(), buildKey(), inputStream, metadata);
        this.client.putObject(request);
    }

    private String buildKey() {
        String fileName = String.format("FooStream-%d.json", eventObject.getOffset());
        return Paths.get(eventObject.getObjectPath(), fileName).toString();
    }

    private String buildObjectContent() {
        StringBuilder sb = new StringBuilder();
        this.eventObject.getEvents().forEach(e -> {
            sb.append(e.toString()).append("\n");
        });
        return sb.toString();
    }
}
