package azkaban.execapp.event;

import azkaban.event.Event;
import azkaban.event.EventListener;
import azkaban.execapp.JobRunner;
import azkaban.executor.Status;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.RestfulApiClient;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import java.net.URI;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

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

        if (! enabled) {
          logger.info("Status updates are not enabled (Azkaban Job Property: 'status.environment=dev|staging|prod')");
          return;
        }

        if (! azkProps.containsKey(STATUS_ENVIRONMENT)) {
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

  /**
   * Sends new event to ALM Status Service
   **/
  private void sendEvent(
      Logger logger,
      JobRunner runner,
      String tenantId,
      String type,
      Map<String, String> tags,
      Map<String, String> results,
      String status) throws IOException {

    logger.info("Status - Send Event - Tenant: " + tenantId + ", Type: " +
        type + ", Status: " + status + ", Tags: " + tags + ", Results: " + results);

    if (tenantId == null) {
      logger.error("No tenantId defined!");
      return;
    }

    if (type == null) {
      logger.error("No event type defined!");
      return;
    }

    if (status == null) {
      logger.error("No event status defined!");
      return;
    }

    // Define uri
    String env = runner.getProps().get(STATUS_ENVIRONMENT);
    String host = "ec-platform-status-" + env + ".sfiqplatform.com";
    int port = 8081;
    boolean isHttp = false;

    // For local testing
    if (env.equalsIgnoreCase("local")) {
      logger.info("Running in local mode...");
      host = "localhost";
      isHttp = true;
    }

    String path = "/api/events/save";
    List<Pair<String, String>> paramsList = new ArrayList<Pair<String, String>>();

    URI uri = StatusApiClient.buildUri(host, port, path,
        isHttp, paramsList.toArray(new Pair[0]));

    // Create JSON for event
    JsonObject json = newEventJson(
        type, status, tenantId, tags, results);

    logger.info(String.format("* Sending POST request with JSON: %s", json.toString()));
    logger.info(String.format("curl -i -XPOST %s -d '%s'", uri.toString(), json.toString()));

    // Send POST request
    StatusApiClient client = new StatusApiClient();
    String response = client.postRequest(uri, json);

    // Log response
    logger.info(String.format("* Received response %s", response));
  }

  /**
   * Private helper method to generate JsonObject for status event
   * 
   * Json for Event object should like this:
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
   * @return GSON JsonObject
   */
  private JsonObject newEventJson(
      String type,
      String status,
      String tenantId,
      Map<String, String> tags,
      Map<String, String> results) {

    Gson gson = new GsonBuilder().create();

    JsonObject json = new JsonObject();

    // Generate random UUID
    String randomId = java.util.UUID.randomUUID().toString();
    json.add("id", withName(randomId));

    // Current time in ISO-8601 representation
    json.addProperty("createdAt", Instant.now().toString());

    json.add("type", withName(type));

    json.addProperty("status", status);

    json.add("tenantId", withName(tenantId));

    // Convert java Hashmap to String and add to JsonObject
    json.add("tags", gson.toJsonTree(tags).getAsJsonObject());

    json.add("results", gson.toJsonTree(results).getAsJsonObject());

    return json;
  }

  // Helper method for generating json string
  private static JsonObject withName(String s) {
    JsonObject obj = new JsonObject();
    obj.addProperty("name", s);
    return obj;
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
