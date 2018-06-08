package net.christophe.genin.vertx.feeder.db;

import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.ResultSet;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.eventbus.Message;
import io.vertx.reactivex.ext.jdbc.JDBCClient;
import io.vertx.reactivex.ext.sql.SQLClient;
import net.christophe.genin.vertx.feeder.Timer;
import net.christophe.genin.vertx.feeder.config.LastId;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Finder extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(Finder.class);
    public static final String SEARCH = Finder.class.getName() + ".run";

    @Override
    public void start() {

        JsonObject dbConfig = config().getJsonObject("dbConfig");
        Objects.requireNonNull(dbConfig, "dbConfig must not be null");

        SQLClient client = JDBCClient.createNonShared(vertx, dbConfig);

        vertx.eventBus().consumer(SEARCH, (m) -> {
            logger.info("search");
            vertx.eventBus().<Long>rxSend(LastId.READ, new JsonObject())
                    .map(Message::body)
                    .flatMap(l -> {
                        // TODO SQL excution
                        if (l == Long.MIN_VALUE) {
                            return client.rxGetConnection()
                                    .flatMap(conn -> conn.rxQuery("SELECT * from DUAL"))
                                    .map(resultSet -> resultSet.getResults())
                                    .map(JsonArray::new);

                        } else {
                            return client.rxGetConnection()
                                    .flatMap(conn -> conn.rxQuery("SELECT * from DUAL"))
                                    .map(resultSet -> resultSet.getResults())
                                    .map(JsonArray::new);
                        }
                    })

                    .subscribe(
                            json -> {
                                vertx.eventBus().send(LastId.SAVE, json);
                            }, err -> {
                                logger.error("Error in finding data ", err);
                                vertx.eventBus().send(Timer.RELAUNCH, new JsonObject());
                            });
        });
        logger.info("run");
    }


    private static Single<JsonArray> select(SQLClient client, String sql, JsonArray params) {
        return client.rxGetConnection()
                .flatMap(conn -> conn.rxQueryWithParams(sql, params)
                        .subscribeOn(Schedulers.io())
                        .doAfterTerminate(conn::close)
                        .map(resultSet -> {
                            List<JsonArray> results = resultSet.getResults();
                            if (results.isEmpty())
                                return new JsonArray();
                            else {
                                List<String> columnNames = resultSet.getColumnNames();
                                return convertToJsonObject(results, columnNames);
                            }
                        }));
    }

    private static JsonArray convertToJsonObject(List<JsonArray> results, List<String> columnNames) {
        List<JsonObject> objs = results.parallelStream()
                .map(arr -> {
                    JsonObject res = new JsonObject();
                    for (int i = 0; i < columnNames.size(); i++) {
                        res.put(columnNames.get(i), arr.getValue(i));
                    }
                    return res;
                }).collect(Collectors.toList());
        return new JsonArray(objs);
    }
}
