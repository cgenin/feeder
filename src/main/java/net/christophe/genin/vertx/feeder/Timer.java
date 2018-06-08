package net.christophe.genin.vertx.feeder;


import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.reactivex.core.AbstractVerticle;
import net.christophe.genin.vertx.feeder.db.Finder;

public class Timer extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(Timer.class);
    public static final String RELAUNCH = Timer.class.getName() + ".relaunch";

    @Override
    public void start() {
        JsonObject config = config();
        Long delay = config.getLong("delay", 60000L);
        logger.info("delay of the loop : " + delay);
        vertx.eventBus().<JsonObject>consumer(RELAUNCH, (msg) -> {
            JsonObject body = msg.body();
            String lastId = body.getString("lastId", "no");
            Integer nb = body.getInteger("nb", 0);
            logger.info("lastId : " + lastId + " / nb : " + nb);
            launch(delay);
        });
        logger.info("started");
    }

    private void launch(Long delay) {
        vertx.setTimer(delay, (l) -> vertx.eventBus().<JsonObject>send(Finder.SEARCH, new JsonObject()));
    }
}
