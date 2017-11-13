package azkaban.execapp.event;

import azkaban.event.Event;
import azkaban.event.EventListener;
import azkaban.execapp.JobRunner;
import azkaban.executor.Status;
import azkaban.utils.Props;
import org.apache.log4j.Logger;
import status.Types;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static azkaban.executor.Status.SUCCEEDED;

public class StatusEventListener implements EventListener {

    private static final Logger listenerLogger = Logger
            .getLogger(StatusEventListener.class);

    private static final String STATUS_ENABLED = "status.enabled";
    private static final String STATUS_TENANT = "status.tenant";
    private static final String STATUS_TYPE = "status.type";
    private static final String STATUS_ENVIRONMENT = "status.environment";

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

    /**
     * Gets tags.
     *
     * @return the tags
     */
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

    private void alertJobStarted(Logger logger, JobRunner runner, Event event) throws Exception {
        Types.Event.Status status = Types.Event.Status.PENDING;

        alert(logger, runner, event, status);
    }

    private void alertJobFinished(Logger logger, JobRunner runner, Event event) throws Exception {
        Types.Event.Status status = Types.Event.Status.SUCCESSFUL;
        if (Status.isStatusFinished(runner.getStatus()) && !runner.getStatus().equals(SUCCEEDED)) status = Types.Event.Status.FAILURE;

        alert(logger, runner, event, status);
    }

    public void alert(Logger logger, JobRunner runner, Event event, Types.Event.Status status) throws Exception {
        Map<String, String> tags = getTags(logger, runner);

        Types.Event.TenantId tenantId = getTenant(runner);
        Types.Event.Type type = getType(runner);

        Map<String, String> results = Collections.emptyMap();

        sendEvent(logger, runner, tenantId, type, tags, results, status);
    }

    private Types.EventReceivedResponse sendEvent(
            Logger logger,
            JobRunner runner,
            Types.Event.TenantId tenantId,
            Types.Event.Type type,
            Map<String, String> tags,
            Map<String, String> results,
            Types.Event.Status status) {

        logger.info("Status - Send Event - Tenant: " + tenantId + ", Type: " + type + ", Status: " + status + ", Tags: " + tags + ", Results: " + results);

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

        if (tags == null) tags = Collections.emptyMap();
        if (results == null) results = Collections.emptyMap();

        Types.Event.UUID id = Types.Event.UUID.newBuilder()
                .setName(java.util.UUID.randomUUID().toString())
                .build();

        Instant time = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(time.getEpochSecond())
                .setNanos(time.getNano()).build();

        Types.Event e1 = Types.Event.newBuilder()
                .setId(id)
                .setType(type)
                .putAllTags(tags)
                .setCreatedAt(timestamp)
                .setStatus(status)
                .putAllResults(results)
                .setTenantId(tenantId)
                .build();

        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext(true).build();

        StatusEventSenderGrpc.StatusEventSenderBlockingStub client = StatusEventSenderGrpc.newBlockingStub(channel);

        Types.EventReceivedResponse response = client.saveSingleEvent(e1);
        logger.info("Status Update - Response: " + response);

        return response;
    }

    private Types.Event.TenantId getTenant(JobRunner runner)  {
        if (!runner.getProps().containsKey(STATUS_TENANT)) return null;
        return Types.Event.TenantId.newBuilder()
                .setName(runner.getProps().get(STATUS_TENANT))
                .build();
    }

    private Types.Event.Type getType(JobRunner runner)  {
        if (!runner.getProps().containsKey(STATUS_TYPE)) return null;
        return Types.Event.Type.newBuilder()
                .setName(runner.getProps().get(STATUS_TYPE))
                .build();
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