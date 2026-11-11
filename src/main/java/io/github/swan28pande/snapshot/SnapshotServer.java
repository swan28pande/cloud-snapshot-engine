package io.github.swan28pande.snapshot;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;

/** Entry point: {@code java -jar snapshot-engine.jar} (configured by environment variables). */
public final class SnapshotServer {
    private static final Logger log = Logger.getLogger(SnapshotServer.class.getName());

    public static void main(String[] args) throws Exception {
        Config cfg = Config.fromEnv();
        SnapshotEngine engine = new SnapshotEngine(cfg.openStore(), cfg.blockSize(), cfg.uploadThreads());
        if (cfg.allowedPaths().isEmpty()) {
            log.warning("ALLOWED_PATHS is unset: clients can read/write any path this process can access");
        }

        Server server = ServerBuilder.forPort(cfg.port())
                .addService(new SnapshotGrpcService(engine, cfg.allowedPaths()))
                .addService(ProtoReflectionServiceV1.newInstance())
                .build()
                .start();
        log.info(String.format("snapshot engine listening on :%d (store=%s, block size=%d KiB)",
                cfg.port(), cfg.store(), cfg.blockSize() / 1024));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.shutdown();
            try {
                server.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            engine.close();
        }));
        server.awaitTermination();
    }
}
