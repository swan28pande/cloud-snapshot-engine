package io.github.swan28pande.snapshot.store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/** Filesystem-backed store with the same key layout as S3, for local development. */
public final class LocalObjectStore implements ObjectStore {
    private final Path root;

    public LocalObjectStore(Path root) {
        this.root = root;
    }

    private Path path(String key) {
        Path p = root.resolve(key).normalize();
        if (!p.startsWith(root.normalize())) throw new IllegalArgumentException("bad key: " + key);
        return p;
    }

    @Override
    public boolean exists(String key) {
        return Files.exists(path(key));
    }

    @Override
    public void put(String key, byte[] data) throws IOException {
        Path target = path(key);
        Files.createDirectories(target.getParent());
        // Write then rename, so readers never see a partial object.
        Path tmp = Files.createTempFile(target.getParent(), ".put", ".tmp");
        Files.write(tmp, data);
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    @Override
    public Optional<byte[]> get(String key) throws IOException {
        try {
            return Optional.of(Files.readAllBytes(path(key)));
        } catch (NoSuchFileException e) {
            return Optional.empty();
        }
    }

    @Override
    public List<String> list(String prefix) throws IOException {
        Path dir = path(prefix);
        if (!Files.isDirectory(dir)) return List.of();
        try (Stream<Path> s = Files.list(dir)) {
            return s.filter(Files::isRegularFile)
                    .map(p -> root.relativize(p).toString().replace('\\', '/'))
                    .sorted()
                    .toList();
        }
    }
}
