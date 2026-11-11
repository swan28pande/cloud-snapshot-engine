package io.github.swan28pande.snapshot;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import io.github.swan28pande.snapshot.store.LocalObjectStore;
import io.github.swan28pande.snapshot.store.ObjectStore;
import io.github.swan28pande.snapshot.store.S3ObjectStore;

/** Environment-variable configuration. */
record Config(
        int port,
        int blockSize,
        int uploadThreads,
        List<Path> allowedPaths,
        String store,
        String s3Bucket,
        String s3Prefix,
        String s3Endpoint,
        Path localDir) {

    static Config fromEnv() {
        String allowed = env("ALLOWED_PATHS", "");
        return new Config(
                Integer.parseInt(env("PORT", "50051")),
                Integer.parseInt(env("BLOCK_SIZE", String.valueOf(4 * 1024 * 1024))),
                Integer.parseInt(env("UPLOAD_THREADS", "8")),
                allowed.isBlank() ? List.of()
                        : Arrays.stream(allowed.split(",")).map(String::trim).map(Path::of).toList(),
                env("STORE", "local"),
                env("S3_BUCKET", ""),
                env("S3_PREFIX", ""),
                env("S3_ENDPOINT", ""),
                Path.of(env("LOCAL_STORE_DIR", "./snapshot-store")).toAbsolutePath());
    }

    ObjectStore openStore() {
        return switch (store) {
            case "s3" -> {
                if (s3Bucket.isBlank()) throw new IllegalStateException("STORE=s3 requires S3_BUCKET");
                yield new S3ObjectStore(s3Bucket, s3Prefix, s3Endpoint);
            }
            case "local" -> new LocalObjectStore(localDir);
            default -> throw new IllegalStateException("STORE must be 's3' or 'local', got " + store);
        };
    }

    private static String env(String name, String dflt) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? dflt : v;
    }
}
