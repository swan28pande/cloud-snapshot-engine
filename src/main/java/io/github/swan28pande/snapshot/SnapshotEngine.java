package io.github.swan28pande.snapshot;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import com.google.protobuf.InvalidProtocolBufferException;

import io.github.swan28pande.snapshot.proto.Manifest;
import io.github.swan28pande.snapshot.proto.SnapshotInfo;
import io.github.swan28pande.snapshot.store.ObjectStore;

/**
 * Block-level snapshots with content-addressed deduplication.
 *
 * <p>A volume is read in fixed-size blocks. Each block is hashed (SHA-256); all-zero blocks are
 * recorded but never stored, and any other block is uploaded only if no snapshot of any volume
 * has stored that content before. A snapshot is therefore incremental against everything already
 * in the store, not just its parent. The manifest (ordered list of block hashes) is all that's
 * needed to reassemble the image.
 */
public final class SnapshotEngine {
    private static final Pattern ID = Pattern.compile("[A-Za-z0-9._-]{1,128}");
    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

    private final ObjectStore store;
    private final int blockSize;
    private final ExecutorService uploads;
    private final int uploadThreads;

    public SnapshotEngine(ObjectStore store, int blockSize, int uploadThreads) {
        this.store = store;
        this.blockSize = blockSize;
        this.uploadThreads = uploadThreads;
        this.uploads = Executors.newFixedThreadPool(uploadThreads);
    }

    // ------------------------------------------------------------------ create

    public SnapshotInfo createSnapshot(String volumeId, Path device, String parentId) throws IOException {
        checkId(volumeId);
        Optional<Manifest> parent = parentId.isEmpty() ? latest(volumeId) : Optional.of(manifest(volumeId, parentId));
        List<String> parentHashes = parent.<List<String>>map(Manifest::getBlockHashesList).orElse(List.of());
        if (parent.isPresent() && parent.get().getInfo().getBlockSize() != blockSize) {
            throw new IllegalArgumentException("parent snapshot uses a different block size");
        }

        // Hashes known to be stored already: no HEAD request needed for these.
        Set<String> known = ConcurrentHashMap.newKeySet();
        known.addAll(parentHashes);
        known.remove("");

        List<String> hashes = new ArrayList<>();
        AtomicLong newBlocks = new AtomicLong(), bytesUploaded = new AtomicLong();
        long changed = 0, zero = 0, size = 0;
        Semaphore inFlight = new Semaphore(uploadThreads * 2); // bounds memory: in-flight blocks
        List<Future<?>> pending = new ArrayList<>();

        try (FileChannel ch = FileChannel.open(device, StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocate(blockSize);
            for (int i = 0; ; i++) {
                int n = readBlock(ch, buf);
                if (n == 0) break;
                size += n;
                byte[] block = Arrays.copyOf(buf.array(), n);
                String hash = isZero(block) ? "" : sha256(block);
                hashes.add(hash);
                if (i >= parentHashes.size() || !parentHashes.get(i).equals(hash)) changed++;
                if (hash.isEmpty()) {
                    zero++;
                } else if (known.add(hash)) { // first time this run sees it: upload unless stored
                    inFlight.acquireUninterruptibly();
                    pending.add(uploads.submit(() -> {
                        try {
                            String key = ObjectStore.blockKey(hash);
                            if (!store.exists(key)) {
                                store.put(key, block);
                                newBlocks.incrementAndGet();
                                bytesUploaded.addAndGet(block.length);
                            }
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        } finally {
                            inFlight.release();
                        }
                    }));
                }
                if (n < blockSize) break; // short read only at end of volume
            }
        }
        await(pending);

        String snapshotId = "snap-" + LocalDateTime.now().format(TS) + "-"
                + String.format("%04x", ThreadLocalRandom.current().nextInt(0x10000));
        long total = hashes.size();
        SnapshotInfo info = SnapshotInfo.newBuilder()
                .setVolumeId(volumeId)
                .setSnapshotId(snapshotId)
                .setParentSnapshotId(parent.map(m -> m.getInfo().getSnapshotId()).orElse(""))
                .setCreatedAtMs(System.currentTimeMillis())
                .setVolumeSizeBytes(size)
                .setBlockSize(blockSize)
                .setTotalBlocks(total)
                .setChangedBlocks(changed)
                .setNewBlocks(newBlocks.get())
                .setReusedBlocks(total - zero - newBlocks.get())
                .setZeroBlocks(zero)
                .setBytesUploaded(bytesUploaded.get())
                .build();
        // Manifest last: a snapshot only becomes visible once all its blocks are durable.
        Manifest m = Manifest.newBuilder().setInfo(info).addAllBlockHashes(hashes).build();
        store.put(ObjectStore.manifestKey(volumeId, snapshotId), m.toByteArray());
        return info;
    }

    // -------------------------------------------------------------------- list

    public List<SnapshotInfo> listSnapshots(String volumeId) throws IOException {
        checkId(volumeId);
        List<SnapshotInfo> out = new ArrayList<>();
        for (String key : store.list(ObjectStore.manifestPrefix(volumeId))) {
            if (!key.endsWith(".pb")) continue;
            store.get(key).map(SnapshotEngine::parse).ifPresent(m -> out.add(m.getInfo()));
        }
        out.sort((a, b) -> Long.compare(a.getCreatedAtMs(), b.getCreatedAtMs()));
        return out;
    }

    // ----------------------------------------------------------------- restore

    /** Reassemble a snapshot into {@code target}: a regular file (written sparse) or a block device. */
    public long[] restore(String volumeId, String snapshotId, Path target) throws IOException {
        Manifest m = manifest(volumeId, snapshotId);
        boolean regularFile = !Files.exists(target) || Files.isRegularFile(target);
        long[] fetched = {0};
        try (FileChannel out = FileChannel.open(target, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            if (regularFile) out.truncate(0);
            forEachBlock(m, (offset, data, zero) -> {
                try {
                    if (zero && regularFile) return; // leave a hole
                    ByteBuffer src = ByteBuffer.wrap(data);
                    while (src.hasRemaining()) out.write(src, offset + src.position());
                    if (!zero) fetched[0]++;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            long size = m.getInfo().getVolumeSizeBytes();
            if (regularFile && out.size() < size) {
                out.write(ByteBuffer.allocate(1), size - 1); // extend over a trailing hole
            }
            out.force(true);
        }
        return new long[] {m.getInfo().getVolumeSizeBytes(), fetched[0]};
    }

    @FunctionalInterface
    public interface BlockSink {
        /** {@code data} is the block content; for zero blocks it is a zero-filled array. */
        void accept(long offset, byte[] data, boolean zero);
    }

    /** Fetch and verify every block of a snapshot, in order. */
    public void forEachBlock(Manifest m, BlockSink sink) throws IOException {
        long bs = m.getInfo().getBlockSize();
        long size = m.getInfo().getVolumeSizeBytes();
        List<String> hashes = m.getBlockHashesList();
        for (int i = 0; i < hashes.size(); i++) {
            long offset = i * bs;
            int len = (int) Math.min(bs, size - offset);
            String hash = hashes.get(i);
            if (hash.isEmpty()) {
                sink.accept(offset, new byte[len], true);
                continue;
            }
            byte[] data = store.get(ObjectStore.blockKey(hash))
                    .orElseThrow(() -> new IOException("missing block " + hash));
            if (!sha256(data).equals(hash)) throw new IOException("corrupt block " + hash);
            sink.accept(offset, data, false);
        }
    }

    public Manifest manifest(String volumeId, String snapshotId) throws IOException {
        checkId(volumeId);
        checkId(snapshotId);
        return store.get(ObjectStore.manifestKey(volumeId, snapshotId))
                .map(SnapshotEngine::parse)
                .orElseThrow(() -> new NoSuchSnapshotException(volumeId + "/" + snapshotId));
    }

    // ------------------------------------------------------------------ helpers

    private Optional<Manifest> latest(String volumeId) throws IOException {
        List<SnapshotInfo> all = listSnapshots(volumeId);
        if (all.isEmpty()) return Optional.empty();
        return Optional.of(manifest(volumeId, all.get(all.size() - 1).getSnapshotId()));
    }

    private static int readBlock(FileChannel ch, ByteBuffer buf) throws IOException {
        buf.clear();
        while (buf.hasRemaining()) {
            if (ch.read(buf) < 0) break;
        }
        return buf.position();
    }

    private static boolean isZero(byte[] b) {
        for (byte x : b) if (x != 0) return false;
        return true;
    }

    static String sha256(byte[] data) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Manifest parse(byte[] bytes) {
        try {
            return Manifest.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void checkId(String id) {
        if (!ID.matcher(id).matches()) throw new IllegalArgumentException("invalid id: " + id);
    }

    private static void await(List<Future<?>> futures) throws IOException {
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted", e);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UncheckedIOException u) throw u.getCause();
                throw new IOException(e.getCause());
            }
        }
    }

    public void close() {
        uploads.shutdown();
    }

    public static final class NoSuchSnapshotException extends RuntimeException {
        NoSuchSnapshotException(String what) {
            super("no such snapshot: " + what);
        }
    }
}
