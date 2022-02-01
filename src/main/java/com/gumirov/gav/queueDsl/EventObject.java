package com.gumirov.gav.queueDsl;

import org.d0sl.model.BaseObject;
import org.json.JSONException;
import org.json.JSONObject;

public class EventObject extends BaseObject {
    private final JSONObject obj;

    public EventObject(String object) throws JSONException {
        obj = new JSONObject(object);
    }

    public String getString(String key) throws JSONException {
        return obj.getString(key);
    }
}
