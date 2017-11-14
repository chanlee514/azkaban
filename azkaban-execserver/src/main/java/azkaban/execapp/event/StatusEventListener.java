package azkaban.execapp.event;

import azkaban.event.Event;
import azkaban.event.EventListener;
import azkaban.execapp.JobRunner;
import azkaban.executor.Status;
import azkaban.utils.Props;
import azkaban.utils.RestfulApiClient;
import org.apache.log4j.Logger;
import status.Types;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import com.google.gson.JsonObject;

import static azkaban.executor.Status.SUCCEEDED;

public class StatusEventListener implements EventListener {

    private static final Logger listenerLogger = Logger
            .getLogger(StatusEventListener.class);

    private static final String STATUS_ENABLED = "status.enabled";
    private static final String STATUS_TENANT = "status.tenant";
    private static final String STATUS_TYPE = "status.type";
    private static final String STATUS_ENVIRONMENT = "status.environment";

    private static final String EVENT_STATUS_PENDING = "PENDING";
    private static final String EVENT_STATUS_SUCCESSFUL = "SUCCESSFUL";
    private static final String EVENT_STATUS_FAILURE = "FAILURE";

    @Override
    public void handleEvent(Event event) {
        if (event.getRunner() instanceof JobRunner) {
            try {
                JobRunner runner = (JobRunner)event.getRunner();
                Props azkProps = runner.getProps();

                Logger logger = runner.getLogger();
                if (logger == null) logger = listenerLogger;

                boolean enabled = azkProps.getBoolean(STATUS_ENABLED, false);

                if (!enabled) {
                    logger.info("Status updates are not enabled (Azkaban Job Property: 'status.environment=dev|staging|prod')");
                    return;
                }

                if (!azkProps.containsKey(STATUS_ENVIRONMENT)) {
                    logger.info("Status Environment is not enabled (Azkaban Job Property: 'status.enabled=true|false')");
                    return;
                }

                logger.info("Einstein Status Alerter - Event: " + event);

                logger.info("** Azkaban Job Properties ** ");
                for (String key : azkProps.getKeySet()) {
                    String value = azkProps.get(key);
                    logger.info(key + ": " + value);
                }

                if (event.getType() == Event.Type.JOB_STARTED) {
                    alertJobStarted(logger, runner, event);
                } else if (event.getType() == Event.Type.JOB_FINISHED) {
                    alertJobFinished(logger, runner, event);
                }

            } catch (Throwable e) {
                // Use job runner logger so user can see the issue in their job log
                JobRunner jobRunner = (JobRunner) event.getRunner();
                jobRunner.getLogger().error(
                        "Encountered error while handling status update event", e);
            }

        } else {
            listenerLogger.warn("((( Got an unsupported runner: "
                    + event.getRunner().getClass().getName() + " )))");
        }

    }

    private void alertJobStarted(
            Logger logger,
            JobRunner runner,
            Event event) throws Exception {

        alert(logger, runner, event, EVENT_STATUS_PENDING);
    }

    private void alertJobFinished(
            Logger logger,
            JobRunner runner,
            Event event) throws Exception {

        if (Status.isStatusFinished(runner.getStatus()) &&
            !runner.getStatus().equals(SUCCEEDED)) {

            alert(logger, runner, event, EVENT_STATUS_FAILURE);
        } else {
            alert(logger, runner, event, EVENT_STATUS_SUCCESSFUL);
        }
    }

    public void alert(
            Logger logger,
            JobRunner runner,
            Event event,
            String status) throws Exception {

        Map<String, String> tags = this.getTags(logger, runner);
        if (tags == null) tags = Collections.emptyMap();

        Map<String, String> results = Collections.emptyMap();

        String tenantId = this.getTenant(runner);
        String type = this.getType(runner);

        sendEvent(logger, runner, tenantId, type, tags, results, status);
    }

    private JsonObject sendEvent(
            Logger logger,
            JobRunner runner,
            String tenantId,
            String type,
            Map<String, String> tags,
            Map<String, String> results,
            String status) {

        logger.info("Status - Send Event - Tenant: " + tenantId + ", Type: " +
            type + ", Status: " + status + ", Tags: " + tags + ", Results: " + results);

        if (tenantId == null) {
            logger.error("No tenantId defined!");
            return null;
        }

        if (type == null) {
            logger.error("No event type defined!");
            return null;
        }

        if (status == null) {
            logger.error("No event status defined!");
            return null;
        }

        String env = runner.getProps().get(STATUS_ENVIRONMENT);
        String host = "ec-platform-status-" + env + ".sfiqplatform.com";
        int port = 80;
        String path = "/api/events/save";

        JsonObject json = this.newEventJson(
            type, status, tenantId, tags, results);

        // TODO send REST POST call using org.apache.http.client.HttpClient

        return response;
    }

    /**
     * Private helper method to create event JsonObject
     * For use in calling status service REST API
     * 
     * Event class (protobuf generated) looks like this:
     * {
     *   "id": {"name": "5ff38550-e976-4fb6-889a-46dcd508f6a8"},
     *   "type": {"name": "modeling_job"},
     *   "tags": {"app": "builder", "tenant": "test_org"},
     *   "createdAt": "1970-01-01T00:00:00Z",
     *   "status": "INITIALIZING",
     *   "results": {"datasetId": "1"},
     *   "tenantId": {"name": "tenant1"}
     * }
     * 
     * @return JsonObject
     */
    private JsonObject newEventJson(
            String type,
            String status,
            String tenantId,
            Map<String, String> tags,
            Map<String, String> results) {

        JsonObject json = new JsonObject();

        // Generate random UUID
        String randomId = java.util.UUID.randomUUID().toString();
        json.addProperty("id", withName(randomId));

        // Current time in ISO-8601 representation
        json.addProperty("createdAt", Instant.now().toString());

        json.addProperty("type", withName(type));

        json.addProperty("status", status);

        json.addProperty("tenantId", withName(tenantId));

        // Convert java Hashmap to String and add to JsonObject
        Gson gson = new GsonBuilder().create();

        json.addProperty("tags", gson.toJson(tags));

        json.addProperty("results", gson.toJson(results));

        return json;
    }

    // Helper method for generating json string
    private static String withName(String s) {
        return String.format("{\"name\": \"%s\"}", s)
    }

    /*
     * Private helper methods for getting Azkaban properties
     */
    private String getTenant(JobRunner runner)  {
        if (!runner.getProps().containsKey(STATUS_TENANT)) return null;
        return runner.getProps().get(STATUS_TENANT);
    }
    private String getType(JobRunner runner)  {
        if (!runner.getProps().containsKey(STATUS_TYPE)) return null;
        return runner.getProps().get(STATUS_TYPE);
    }
    public Map<String, String> getTags(Logger logger, JobRunner runner) {
        Map<String, String> tags = new HashMap<String, String>();

        Props azkProps = runner.getProps();

        if (azkProps != null) {
            for (String key : azkProps.getKeySet()) {
                String value = azkProps.get(key);
                if (key.startsWith("status.tag.")) {
                    String tag = key.substring("status.tag.".length());
                    tags.put(tag, value);
                } else {
                    tags.put("azkaban.props." + key, value);
                }
            }
        }

        return tags;
    }

    /**
     * Join string.
     *
     * @param strings   the strings
     * @param separator the separator
     * @return the string
     */
    public static String join(String[] strings, String separator) {
        StringBuilder sb = new StringBuilder();
        String sep = "";
        for (String s : strings) {
            sb.append(sep).append(s);
            sep = separator;
        }
        return sb.toString();
    }
}