package io.github.swan28pande.snapshot.store;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Optional;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

/** S3 (or any S3-compatible store, e.g. MinIO via {@code endpoint}). */
public final class S3ObjectStore implements ObjectStore {
    private final S3Client s3;
    private final String bucket;
    private final String prefix;

    public S3ObjectStore(String bucket, String prefix, String endpoint) {
        S3ClientBuilder builder = S3Client.builder(); // region + credentials from the default chain
        if (endpoint != null && !endpoint.isBlank()) {
            builder.endpointOverride(URI.create(endpoint)).forcePathStyle(true);
        }
        this.s3 = builder.build();
        this.bucket = bucket;
        this.prefix = prefix == null || prefix.isBlank() ? "" : prefix.replaceAll("/+$", "") + "/";
    }

    @Override
    public boolean exists(String key) throws IOException {
        try {
            s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(prefix + key).build());
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        } catch (S3Exception e) {
            if (e.statusCode() == 404) return false;
            throw new IOException("S3 HEAD " + key + " failed", e);
        }
    }

    @Override
    public void put(String key, byte[] data) throws IOException {
        try {
            s3.putObject(b -> b.bucket(bucket).key(prefix + key), RequestBody.fromBytes(data));
        } catch (S3Exception e) {
            throw new IOException("S3 PUT " + key + " failed", e);
        }
    }

    @Override
    public Optional<byte[]> get(String key) throws IOException {
        try {
            return Optional.of(s3.getObjectAsBytes(b -> b.bucket(bucket).key(prefix + key)).asByteArray());
        } catch (NoSuchKeyException e) {
            return Optional.empty();
        } catch (S3Exception e) {
            throw new IOException("S3 GET " + key + " failed", e);
        }
    }

    @Override
    public List<String> list(String keyPrefix) throws IOException {
        try {
            return s3.listObjectsV2Paginator(b -> b.bucket(bucket).prefix(prefix + keyPrefix).delimiter("/"))
                    .contents().stream()
                    .map(S3Object::key)
                    .map(k -> k.substring(prefix.length()))
                    .sorted()
                    .toList();
        } catch (S3Exception e) {
            throw new IOException("S3 LIST " + keyPrefix + " failed", e);
        }
    }
}
