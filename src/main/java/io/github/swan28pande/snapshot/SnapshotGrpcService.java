package io.github.swan28pande.snapshot;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;

import com.google.protobuf.ByteString;

import io.github.swan28pande.snapshot.proto.CreateSnapshotRequest;
import io.github.swan28pande.snapshot.proto.ListSnapshotsRequest;
import io.github.swan28pande.snapshot.proto.ListSnapshotsResponse;
import io.github.swan28pande.snapshot.proto.Manifest;
import io.github.swan28pande.snapshot.proto.ReadSnapshotRequest;
import io.github.swan28pande.snapshot.proto.RestoreVolumeRequest;
import io.github.swan28pande.snapshot.proto.RestoreVolumeResponse;
import io.github.swan28pande.snapshot.proto.SnapshotInfo;
import io.github.swan28pande.snapshot.proto.SnapshotServiceGrpc;
import io.github.swan28pande.snapshot.proto.VolumeChunk;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

final class SnapshotGrpcService extends SnapshotServiceGrpc.SnapshotServiceImplBase {
    private static final int CHUNK = 1 << 20; // stay well under gRPC's 4 MiB default message limit

    private final SnapshotEngine engine;
    private final List<Path> allowedPaths;

    SnapshotGrpcService(SnapshotEngine engine, List<Path> allowedPaths) {
        this.engine = engine;
        this.allowedPaths = allowedPaths;
    }

    @Override
    public void createSnapshot(CreateSnapshotRequest req, StreamObserver<SnapshotInfo> resp) {
        call(resp, () -> engine.createSnapshot(
                req.getVolumeId(), checkPath(req.getDevicePath()), req.getParentSnapshotId()));
    }

    @Override
    public void listSnapshots(ListSnapshotsRequest req, StreamObserver<ListSnapshotsResponse> resp) {
        call(resp, () -> ListSnapshotsResponse.newBuilder()
                .addAllSnapshots(engine.listSnapshots(req.getVolumeId())).build());
    }

    @Override
    public void restoreVolume(RestoreVolumeRequest req, StreamObserver<RestoreVolumeResponse> resp) {
        call(resp, () -> {
            long[] r = engine.restore(req.getVolumeId(), req.getSnapshotId(), checkPath(req.getTargetPath()));
            return RestoreVolumeResponse.newBuilder().setBytesWritten(r[0]).setBlocksFetched(r[1]).build();
        });
    }

    @Override
    public void readSnapshot(ReadSnapshotRequest req, StreamObserver<VolumeChunk> resp) {
        var call = (ServerCallStreamObserver<VolumeChunk>) resp;
        try {
            Manifest m = engine.manifest(req.getVolumeId(), req.getSnapshotId());
            engine.forEachBlock(m, (offset, data, zero) -> {
                if (call.isCancelled()) throw new IllegalStateException("cancelled by client");
                if (zero) {
                    resp.onNext(VolumeChunk.newBuilder().setOffset(offset).setZero(true).setLength(data.length).build());
                    return;
                }
                for (int pos = 0; pos < data.length; pos += CHUNK) {
                    int len = Math.min(CHUNK, data.length - pos);
                    resp.onNext(VolumeChunk.newBuilder()
                            .setOffset(offset + pos)
                            .setData(ByteString.copyFrom(data, pos, len))
                            .setLength(len)
                            .build());
                }
            });
            resp.onCompleted();
        } catch (Exception e) {
            if (!call.isCancelled()) resp.onError(toStatus(e));
        }
    }

    // ------------------------------------------------------------------ helpers

    private interface Body<T> {
        T run() throws IOException;
    }

    private static <T> void call(StreamObserver<T> resp, Body<T> body) {
        try {
            resp.onNext(body.run());
            resp.onCompleted();
        } catch (Exception e) {
            resp.onError(toStatus(e));
        }
    }

    private static io.grpc.StatusRuntimeException toStatus(Exception e) {
        Throwable cause = e instanceof UncheckedIOException u ? u.getCause() : e;
        Status s;
        if (cause instanceof SnapshotEngine.NoSuchSnapshotException) s = Status.NOT_FOUND;
        else if (cause instanceof IllegalArgumentException) s = Status.INVALID_ARGUMENT;
        else if (cause instanceof SecurityException) s = Status.PERMISSION_DENIED;
        else if (cause instanceof java.nio.file.NoSuchFileException) s = Status.NOT_FOUND;
        else s = Status.INTERNAL;
        return s.withDescription(String.valueOf(cause.getMessage())).withCause(cause).asRuntimeException();
    }

    /** Device/target paths are server-side, so restrict them when ALLOWED_PATHS is set. */
    private Path checkPath(String raw) {
        if (raw.isBlank()) throw new IllegalArgumentException("path is required");
        Path p = Path.of(raw).toAbsolutePath().normalize();
        if (!allowedPaths.isEmpty() && allowedPaths.stream().noneMatch(a -> p.startsWith(a.toAbsolutePath().normalize()))) {
            throw new SecurityException("path not under ALLOWED_PATHS: " + p);
        }
        return p;
    }
}
