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
    private final JSONObject obj;
    private final JSONObject payload;

    public EventObject(JSONObject object) {
        this.obj = object;
        this.payload = object.getJSONObject("payload");
    }

    public String getString(String key) throws JSONException {
        return payload.getString(key);
    }

    public Double getDouble(String key) throws JSONException {
        return payload.getDouble(key);
    }

    public String getUri() {
        return obj.getString("payloadUri");
    }
}
