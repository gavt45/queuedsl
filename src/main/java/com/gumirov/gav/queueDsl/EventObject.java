package com.gumirov.gav.queueDsl;

import com.gumirov.gav.Owl2JSONSchemaAPI;
import org.d0sl.model.BaseObject;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

public class EventObject extends BaseObject {
    private final Schema baseSchema = SchemaLoader.load(new JSONObject("{\n" +
            "  \"$id\" : \"http://d0sl.org/onto#EventObject\",\n" +
            "  \"required\":[\n" +
            "  \t\"system\",\n" +
            "    \"timestamp\",\n" +
            "    \"payloadUri\",\n" +
            "    \"payload\"\n" +
            "  ],\n" +
            "  \"properties\": {\n" +
            "    \"system\" : {\n" +
            "    \t\"type\": \"string\"\n" +
            "    },\n" +
            "    \"timestamp\" : {\n" +
            "    \t\"type\": \"integer\",\n" +
            "        \"example\": 1643738752\n" +
            "    },\n" +
            "    \"payloadUri\" : {\n" +
            "    \t\"type\": \"string\",\n" +
            "        \"pattern\": \"^http(s)?://.*$\"\n" +
            "    },\n" +
            "    \"payload\" : {\n" +
            "    \t\"type\": \"object\"\n" +
            "    }\n" +
            "  },\n" +
            "  \"type\": \"object\"\n" +
            "}"));
    private final JSONObject obj;
    private String uri;
    private String API_URI;

    private Schema fetchSchema(String payloadUri) throws IOException, QueueException {
        return SchemaLoader.load(Owl2JSONSchemaAPI.fetchJson(payloadUri, API_URI));
    }

    private void validatePayload(JSONObject obj) throws IOException, QueueException {
        uri = obj.getString("payloadUri");
        JSONObject payload = obj.getJSONObject("payload");
        Schema schema = fetchSchema(uri);
        schema.validate(payload);
    }

    public EventObject(String object, String OWL2JSONURI) throws JSONException, ValidationException, IOException, QueueException {
        this.API_URI = OWL2JSONURI;
        obj = new JSONObject(object);
        baseSchema.validate(obj);
        validatePayload(obj);
    }

    public String getString(String key) throws JSONException {
        return obj.getString(key);
    }

    public String getUri() {
        return uri;
    }
}
