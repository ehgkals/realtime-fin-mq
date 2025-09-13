package com.realtimefinmq.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class JsonUtils {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonUtils() { }

    /** 객체 -> JSON 문자열 (실패 시 RuntimeException) */
    public static String toJson(Object obj) {
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("JSON serialize failed: " + e.getMessage(), e);
        }
    }
}
