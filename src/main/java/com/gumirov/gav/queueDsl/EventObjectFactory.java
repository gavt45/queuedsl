package com.gumirov.gav.queueDsl;

import com.gumirov.gav.Owl2JSONSchemaAPI;
import com.gumirov.gav.QueueDSL;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;

public class EventObjectFactory {
    private static final Logger log = LogManager.getLogger(EventObjectFactory.class);
    private final long UPDATE_INTERVAL = 1000 * 20 * 1; // every minute
    private Schema baseSchema;
    private String API_URI, BASE_OBJECT_URI;
    private boolean needValidation;
    private HashMap<String, Schema> schemaMap = new HashMap<>();
    private HashMap<String, Long> lastUpdate = new HashMap<>();

    private Schema fetchSchema(String payloadUri) throws IOException, QueueException {
        if (schemaMap.containsKey(payloadUri) && lastUpdate.containsKey(payloadUri) &&
                System.currentTimeMillis() - lastUpdate.get(payloadUri) < UPDATE_INTERVAL) {
            return schemaMap.get(payloadUri);
        } else {
            log.info("Updating schema for " + payloadUri);
            lastUpdate.put(payloadUri, System.currentTimeMillis());
            return SchemaLoader.load(Owl2JSONSchemaAPI.fetchJson(payloadUri, API_URI));
        }
    }

    private void validatePayload(JSONObject obj) throws IOException, QueueException {
        JSONObject payload = obj.getJSONObject("payload");
        Schema schema = fetchSchema(obj.getString("payloadUri"));
        schema.validate(payload);
    }

    public EventObjectFactory() {
        this.needValidation = false;
        this.baseSchema = null;
    }

    public EventObjectFactory(String OWL2JSON_URI, String BASE_OBJECT_URI) throws QueueException, IOException {
        this.needValidation = true;
        this.API_URI = OWL2JSON_URI;
        this.BASE_OBJECT_URI = BASE_OBJECT_URI;
        this.baseSchema = fetchSchema(BASE_OBJECT_URI);
    }

    public void reFetchSchema() throws QueueException, IOException {
        this.baseSchema = fetchSchema(this.BASE_OBJECT_URI);
    }

    public EventObject makeEventObject(String jsonStr) throws QueueException, IOException {
        JSONObject obj = new JSONObject(jsonStr);
        if (needValidation) {
            baseSchema.validate(obj);
            validatePayload(obj);
        }
        return new EventObject(obj);
    }
}
