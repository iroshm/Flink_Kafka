package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import com.google.gson.Gson;

public class JSONStringToOrderMapper implements MapFunction<String, Order>
{
    private transient Gson gson;

    @Override
    public Order map(String jsonString) throws Exception {
        // Lazily initialize Gson to ensure thread safety
        if (gson == null) {
            gson = new Gson();
        }

        try {
            // Use Gson to deserialize the JSON string into an Order object
            return gson.fromJson(jsonString, Order.class);
        } catch (Exception e) {
            // Handle any parsing errors here
            e.printStackTrace();
            return null; // or throw an exception, log, etc. based on your error handling strategy
        }
    }
}
