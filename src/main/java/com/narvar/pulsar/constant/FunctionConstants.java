package com.narvar.pulsar.constant;

public class FunctionConstants {

    private FunctionConstants() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Other constant
     */

    public static final String DELIMITER_COMMA = ",";

    public static final String CONTENT_TYPE = "Content-type";

    public static final String APPLICATION_JSON = "application/json";

    /**
     * Configuration keys for RedirectToExceptionFunction
     */

    public static final String EVENT_CODES = "event_codes";

    public static final String API_GATEWAY_URL = "api_gateway_url";

    /**
     * Default configuration values for RedirectToExceptionFunction
     */

    public static final String DEFAULT_MESSAGE_SOURCE_LOCATION = "IN";

    public static final String DEFAULT_EVENT_CODE = "700";

    public static final String DEFAULT_API_GATEWAY_URL = "dummy";

}
