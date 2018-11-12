package com.narvar.pulsar.function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.narvar.atlas.api.contracts.kinesis.OrderTrackingUpdate;
import com.narvar.atlas.api.contracts.response.AtlasResponse;
import com.narvar.atlas.api.contracts.response.tracking.Tracking;
import com.narvar.pulsar.exception.BadStatusException;
import com.narvar.pulsar.exception.IllegalStatusException;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.narvar.pulsar.constant.FunctionConstants.*;

/**
 * @Author ankur
 */

public class RedirectToExceptionFunction implements Function<String, Void> {

    private Logger logger;

    private static final int RETRY_ATTEMPTS = 3;

    private static final long BACK_OFF_PERIOD = 3000;

    @Override
    public Void process(String payload, Context context) throws Exception {

        logger = context.getLogger();
        logger.info("Payload received");
        logger.debug(payload);

        String outputTopic = context.getOutputTopic();

        //publish to output topic
        context.publish(outputTopic, payload)
                .thenAccept(v -> logger.info("Message sent successfully to pulsar topic={}", outputTopic));

        //process Atlas response and send to API gateway
        try {
            this.processAtlasMessage(payload, context);
        } catch (Exception e) {
            logger.error("Error: {}", e.getMessage());
            throw e;
        }

        return null;
    }

    /**
     * Processes Atlas response and sends message to Gateway whose EventCode matches the codes provided to the function
     */
    private void processAtlasMessage(String payload, Context context) throws IOException {
        OrderTrackingUpdate orderTrackingUpdate = getOrderTrackingUpdate(payload);
        String sourceLocation = orderTrackingUpdate.getMetaInfo().getMessageSourceLocation();
        logger.info("Carrier's location={}", orderTrackingUpdate.getMetaInfo().getMessageSourceLocation());

        String eventCodes = context.getUserConfigValueOrDefault(EVENT_CODES, DEFAULT_EVENT_CODE).toString();
        logger.info("Event codes supplied for redirecting={}", eventCodes);

        List<String> eventCodeList = Arrays.asList(eventCodes.split(DELIMITER_COMMA));

        String eventCode = this.getEventCode(orderTrackingUpdate);
        logger.info("Event code received in Atlas response={}", eventCode);

        if (DEFAULT_MESSAGE_SOURCE_LOCATION.equals(sourceLocation) && eventCodeList.contains(eventCode)) {
            String gatewayUrl = context.getUserConfigValueOrDefault(API_GATEWAY_URL, DEFAULT_API_GATEWAY_URL).toString();
            if (DEFAULT_API_GATEWAY_URL.equals(gatewayUrl)) {
                logger.error("URL for API Gateway is not provided");
            } else {
                this.sendToGateway(gatewayUrl, payload);
            }
        } else {
            logger.info("Atlas response not eligible for API gateway");
        }
    }

    /**
     * Converts String payload to OrderTrackingUpdate
     */
    private OrderTrackingUpdate getOrderTrackingUpdate(String payload) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
        return mapper.readValue(payload, OrderTrackingUpdate.class);
    }

    /**
     * Returns EventCode from Atlas Response,
     * throws NullPointerException if not found
     */
    private String getEventCode(OrderTrackingUpdate orderTrackingUpdate) {
        AtlasResponse mergedAtlasResponse = orderTrackingUpdate.getMergedOrderTrackingResponse();

        List<Tracking> trackingList;

        if (mergedAtlasResponse == null) {
            logger.info("MergedAtlasResponse was not found in the payload");
            AtlasResponse previousAtlasResponse = orderTrackingUpdate.getPreviousOrderTrackingResponse();
            if (previousAtlasResponse == null) {
                throw new NullPointerException("No atlas response found in the payload");
            } else {
                trackingList = previousAtlasResponse.getTracking();
            }
        } else {
            trackingList = mergedAtlasResponse.getTracking();
        }

        if(trackingList == null || trackingList.isEmpty()) {
            throw new NullPointerException("No tracking details found in Atlas response");
        } else {
            return trackingList.get(0).getCurrentEventCode();
        }
    }

    /**
     * Sends message to API gateway,
     * retry for 3 times with a delay of 3 secs on the event of failure
     */
    private void sendToGateway(String gatewayUrl, String payload) {
        this.createRetryTemplate(RETRY_ATTEMPTS, BACK_OFF_PERIOD).execute((RetryCallback<Void, BadStatusException>) retryContext -> {
            try(CloseableHttpClient httpClient = buildHttpClient()) {
                HttpPost httpPost = new HttpPost(gatewayUrl);
                httpPost.setHeader(CONTENT_TYPE, APPLICATION_JSON);
                StringEntity entity = new StringEntity(payload);
                httpPost.setEntity(entity);
                String response = getResponse(httpClient.execute(httpPost));
                logger.info("Gateway response: {}", response);
                if (!StringUtils.isEmpty(response))
                    logger.info("Message sent to URL={}", gatewayUrl);
            } catch (BadStatusException e) {
                logger.info("Retrying......");
                throw new BadStatusException(e.getMessage());
            } catch (Exception e) {
                logger.error("Error: {}", e.getMessage());
            }
            return null;
        });
    }

    protected CloseableHttpClient buildHttpClient() {
        return HttpClients.createDefault();
    }

    /**
     * Gateway response handler
     */
    private String getResponse(CloseableHttpResponse response) throws IOException {
        int status = response.getStatusLine().getStatusCode();
        if (status >= 200 && status < 300) {
            HttpEntity entity = response.getEntity();
            return entity != null ? EntityUtils.toString(entity) : null;
        } else if (status >= 400 && status < 500) {
            throw new BadStatusException("Unexpected response status: " + status);
        } else {
            throw new IllegalStatusException("Unexpected response status: " + status);
        }
    }

    /**
     * Creates retry template with fixed backoff policy
     */
    private RetryTemplate createRetryTemplate(int retryAttempts, long backOffPeriod) {
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(new SimpleRetryPolicy(retryAttempts, Collections.singletonMap(BadStatusException.class, true)));
        FixedBackOffPolicy policy = new FixedBackOffPolicy();
        policy.setBackOffPeriod(backOffPeriod);
        retryTemplate.setBackOffPolicy(policy);
        return retryTemplate;
    }

}
