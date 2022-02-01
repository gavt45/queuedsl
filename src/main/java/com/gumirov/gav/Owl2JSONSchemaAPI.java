package com.gumirov.gav;

import com.gumirov.gav.queueDsl.QueueException;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

import java.io.*;
import java.net.*;
import java.nio.charset.*;
import java.util.ArrayList;
import java.util.List;

import org.json.*;

public class Owl2JSONSchemaAPI {
    public static JSONObject fetchJson(String classUri, String API_URI) throws IOException, QueueException {
        HttpPost post = new HttpPost(API_URI);

        // add request parameter, form parameters
        List<NameValuePair> urlParameters = new ArrayList<>();
        urlParameters.add(new BasicNameValuePair("iri", classUri));

        post.setEntity(new UrlEncodedFormEntity(urlParameters));

        try (CloseableHttpClient httpClient = HttpClients.createDefault();
             CloseableHttpResponse response = httpClient.execute(post)) {
            if (response.getStatusLine().getStatusCode() != 200){
                throw new QueueException("Can't get json schema! Code: "+response.getStatusLine().getStatusCode());
            }
            return new JSONObject(EntityUtils.toString(response.getEntity()));
        }
    }
}
