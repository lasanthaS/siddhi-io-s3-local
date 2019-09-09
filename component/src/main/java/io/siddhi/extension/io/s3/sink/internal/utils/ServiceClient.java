package io.siddhi.extension.io.s3.sink.internal.utils;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import io.siddhi.extension.io.s3.sink.internal.beans.EventObject;
import io.siddhi.extension.io.s3.sink.internal.beans.SinkConfig;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;

public class ServiceClient {
    private SinkConfig config;
    private AmazonS3 client;

    public ServiceClient(SinkConfig config) {
        this.config = config;
        this.init();
        this.createBucketIfNotExist();
    }

    private void init() {
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
                .withRegion(config.getAwsRegion());
        AWSCredentialsProvider credentialProvider = getCredentialProvider(true);
        if (credentialProvider != null) {
            builder.withCredentials(credentialProvider);
        }
        this.client = builder.build();
    }

    private AWSCredentialsProvider getCredentialProvider(boolean overrideForTesting) {
        if (overrideForTesting) {
            return new ProfileCredentialsProvider();
        }

        if (config.getCredentialProviderClass() != null) {
            try {
                return (AWSCredentialsProvider) this.getClass().getClassLoader().loadClass(config.getCredentialProviderClass()).newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                e.printStackTrace();
            }
            return null;
        }

        if (config.getAwsAccessKey() != null && config.getAwsSecretKey() != null) {
            return new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(config.getAwsAccessKey(), config.getAwsSecretKey()));
        }
        return null;
    }

    public void createBucketIfNotExist() {
        // Check if the bucket exists. If so skip the rest of the code.
        if (this.client.doesBucketExistV2(config.getBucketName())) {
            return;
        }

        // Create the bucket.
        this.client.createBucket(new CreateBucketRequest(config.getBucketName(), config.getAwsRegion()));

        // Enable versioning only if the config flag is set.
        if (config.isVersioningEnabled()) {
            BucketVersioningConfiguration versioningConfiguration = new BucketVersioningConfiguration()
                    .withStatus(BucketVersioningConfiguration.ENABLED);
            this.client.setBucketVersioningConfiguration(
                    new SetBucketVersioningConfigurationRequest(config.getBucketName(), versioningConfiguration));
        }
    }

    public void uploadObject(EventObject eventObject) {
        InputStream inputStream = eventObject.serialize();

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType(eventObject.getContentType());
        try {
            metadata.setContentLength(inputStream.available());
        } catch (IOException e) {
            // Ignore setting content length
        }

        PutObjectRequest request = new PutObjectRequest(eventObject.getBucketName(), buildKey(eventObject),
                inputStream, metadata);
        this.client.putObject(request);
    }

    private String buildKey(EventObject eventObject) {
        String fileName = String.format("FooStream-%d.json", eventObject.getOffset());
        return Paths.get(eventObject.getObjectPath(), fileName).toString();
    }
}
