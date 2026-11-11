package io.github.swan28pande.snapshot;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import io.github.swan28pande.snapshot.proto.CreateSnapshotRequest;
import io.github.swan28pande.snapshot.proto.ListSnapshotsRequest;
import io.github.swan28pande.snapshot.proto.ReadSnapshotRequest;
import io.github.swan28pande.snapshot.proto.RestoreVolumeRequest;
import io.github.swan28pande.snapshot.proto.SnapshotInfo;
import io.github.swan28pande.snapshot.proto.SnapshotServiceGrpc;
import io.github.swan28pande.snapshot.proto.VolumeChunk;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * Small gRPC client.
 *
 * <pre>
 *   snapshot-cli create  VOLUME DEVICE_PATH [PARENT_ID]
 *   snapshot-cli list    VOLUME
 *   snapshot-cli restore VOLUME SNAPSHOT TARGET_PATH     (server writes TARGET_PATH)
 *   snapshot-cli pull    VOLUME SNAPSHOT LOCAL_FILE      (streamed and reassembled here)
 * </pre>
 * Server address from SNAPSHOT_ADDR (default localhost:50051).
 */
public final class SnapshotCli {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) usage();
        String addr = System.getenv().getOrDefault("SNAPSHOT_ADDR", "localhost:50051");
        ManagedChannel ch = ManagedChannelBuilder.forTarget(addr).usePlaintext().build();
        try {
            var stub = SnapshotServiceGrpc.newBlockingStub(ch);
            switch (args[0]) {
                case "create" -> print(stub.createSnapshot(CreateSnapshotRequest.newBuilder()
                        .setVolumeId(args[1]).setDevicePath(arg(args, 2))
                        .setParentSnapshotId(args.length > 3 ? args[3] : "").build()));
                case "list" -> stub.listSnapshots(ListSnapshotsRequest.newBuilder().setVolumeId(args[1]).build())
                        .getSnapshotsList().forEach(SnapshotCli::print);
                case "restore" -> System.out.print(stub.restoreVolume(RestoreVolumeRequest.newBuilder()
                        .setVolumeId(args[1]).setSnapshotId(arg(args, 2)).setTargetPath(arg(args, 3)).build()));
                case "pull" -> pull(stub.readSnapshot(ReadSnapshotRequest.newBuilder()
                        .setVolumeId(args[1]).setSnapshotId(arg(args, 2)).build()), Path.of(arg(args, 3)));
                default -> usage();
            }
        } finally {
            ch.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static void pull(Iterator<VolumeChunk> chunks, Path out) throws IOException {
        long size = 0, bytes = 0;
        try (FileChannel f = FileChannel.open(out, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            while (chunks.hasNext()) {
                VolumeChunk c = chunks.next();
                size = Math.max(size, c.getOffset() + c.getLength());
                if (c.getZero()) continue; // sparse
                ByteBuffer buf = c.getData().asReadOnlyByteBuffer();
                while (buf.hasRemaining()) f.write(buf, c.getOffset() + buf.position());
                bytes += c.getLength();
            }
            if (f.size() < size) f.write(ByteBuffer.allocate(1), size - 1);
        }
        System.out.printf("wrote %s: %d bytes (%d non-zero)%n", out, size, bytes);
    }

    private static void print(SnapshotInfo s) {
        System.out.printf("%s  parent=%s  size=%d  blocks=%d changed=%d new=%d reused=%d zero=%d uploaded=%d%n",
                s.getSnapshotId(), s.getParentSnapshotId().isEmpty() ? "-" : s.getParentSnapshotId(),
                s.getVolumeSizeBytes(), s.getTotalBlocks(), s.getChangedBlocks(), s.getNewBlocks(),
                s.getReusedBlocks(), s.getZeroBlocks(), s.getBytesUploaded());
    }

    private static String arg(String[] args, int i) {
        if (i >= args.length) usage();
        return args[i];
    }

    private static void usage() {
        System.err.println("usage: snapshot-cli create VOLUME DEVICE [PARENT] | list VOLUME | "
                + "restore VOLUME SNAPSHOT TARGET | pull VOLUME SNAPSHOT LOCAL_FILE");
        System.exit(2);
    }
}
