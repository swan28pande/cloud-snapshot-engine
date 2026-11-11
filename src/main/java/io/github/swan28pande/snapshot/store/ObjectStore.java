package io.github.swan28pande.snapshot.store;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Minimal key/value object store. Blocks are content-addressed:
 * {@code blocks/<aa>/<bb>/<sha256>}; manifests live at {@code manifests/<volume>/<snapshot>.pb}.
 */
public interface ObjectStore {
    boolean exists(String key) throws IOException;

    void put(String key, byte[] data) throws IOException;

    Optional<byte[]> get(String key) throws IOException;

    /** Keys directly under {@code prefix}, sorted. */
    List<String> list(String prefix) throws IOException;

    static String blockKey(String sha256) {
        return "blocks/" + sha256.substring(0, 2) + "/" + sha256.substring(2, 4) + "/" + sha256;
    }

    static String manifestKey(String volumeId, String snapshotId) {
        return manifestPrefix(volumeId) + snapshotId + ".pb";
    }

    static String manifestPrefix(String volumeId) {
        return "manifests/" + volumeId + "/";
    }
}
