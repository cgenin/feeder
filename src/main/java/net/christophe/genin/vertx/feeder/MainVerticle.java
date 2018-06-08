package net.christophe.genin.vertx.feeder;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import net.christophe.genin.vertx.feeder.config.LastId;
import net.christophe.genin.vertx.feeder.db.Finder;
import net.christophe.genin.vertx.feeder.es.ElasticSearch;

public class MainVerticle extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(MainVerticle.class);

    @Override
    public void start() {
        vertx.deployVerticle(Timer.class.getName(), new DeploymentOptions().setConfig(config()));
        vertx.deployVerticle(Finder.class.getName(), new DeploymentOptions().setConfig(config()));
        vertx.deployVerticle(LastId.class.getName(), new DeploymentOptions().setConfig(config()));
        vertx.deployVerticle(ElasticSearch.class.getName(), new DeploymentOptions().setConfig(config()));
        logger.info("Runner started");
    }
}
