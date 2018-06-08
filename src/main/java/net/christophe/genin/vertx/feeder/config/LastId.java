package net.christophe.genin.vertx.feeder.config;

import io.reactivex.Single;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.buffer.Buffer;
import net.christophe.genin.vertx.feeder.Timer;


import java.util.Objects;
import java.util.Optional;

public class LastId extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(LastId.class);
    public static final String SAVE = LastId.class.getName() + ".save";
    public static final String READ = LastId.class.getName() + ".read";

    private Optional<Long> lastId = Optional.empty();

    @Override
    public void start() {
        String tempFile = config().getString("tempFile", "temp.db");
        vertx.fileSystem().rxReadFile(tempFile)
                .subscribe(f -> {
                    long l = f.getLong(0);
                    lastId = Optional.of(l);
                    logger.info("lastId : " + l);
                });

        vertx.eventBus().<JsonObject>consumer(SAVE, msg -> {
            JsonObject body = msg.body();
            Single.just(body)
                    .map(json-> json.getString("lastId"))
                    .map(Long::valueOf)
                    .flatMap(l -> vertx.fileSystem().rxWriteFile(tempFile, Buffer.buffer().appendLong(l))
                            .toSingleDefault(true)
                            .onErrorReturnItem(false))
                    .subscribe(
                            v -> {
                                logger.debug("file saved : " + v);
                                vertx.eventBus().send(Timer.RELAUNCH, body);
                            },
                            err -> {
                                logger.error("Error in saving file ", err);
                                vertx.eventBus().send(Timer.RELAUNCH, body);
                            }
                    );
        });
        vertx.eventBus().<JsonObject>consumer(READ, msg -> {
            Long res = lastId.orElse(Long.MIN_VALUE);
            msg.reply(res);
        });

    }
}
