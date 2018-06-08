package net.christophe.genin.vertx.feeder.es;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.reactivex.core.AbstractVerticle;
import net.christophe.genin.vertx.feeder.Timer;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

public class ElasticSearch extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearch.class);
    public static final JsonObject DEFAULT_CONNECTION = new JsonObject().put("hostname", "localhost").put("port", 9200);
    public static final String SAVE = ElasticSearch.class.getName() + ".save";
    public static final String SAVE_LAST_ID = ElasticSearch.class.getName() + ".save.lastId";
    private RestHighLevelClient client;


    @Override
    public void start() throws Exception {

        initializeClient();
        logger.info("Es client created");
        createIndex();
        vertx.eventBus().consumer(SAVE, msg -> {
            // TODO BULK
        });

        logger.info("started");
        vertx.eventBus().send(Timer.RELAUNCH, new JsonObject());
    }

    private void initializeClient() {
        JsonArray esHosts = config().getJsonArray("EsHosts", new JsonArray().add(DEFAULT_CONNECTION));
        logger.info("hosts : " + esHosts.encode());
        HttpHost[] hosts = esHosts.<JsonObject>stream()
                .map(json -> new HttpHost("localhost", 9200, "http"))
                .toArray(HttpHost[]::new);

        client = new RestHighLevelClient(RestClient.builder(hosts));
    }

    private void createIndex() throws IOException {
        String prefixIndexEs = config().getString("prefixIndexEs", "index");
        String time = new SimpleDateFormat("YYYY-MM-dd-HH-mm-ss").format(new Date());
        String index = prefixIndexEs + "-" + time;
        logger.info("create Index '" + index + "' in ES....");
        CreateIndexRequest request = new CreateIndexRequest(index);
        CreateIndexResponse createIndexResponse = client.indices().create(request);
        logger.info("index '" + createIndexResponse.index() + "' created.");
    }

    @Override
    public void stop() throws Exception {
        synchronized (vertx) {
            if (!Objects.isNull(client)) {
                client.close();
            }
        }
    }
}
