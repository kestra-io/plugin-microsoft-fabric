package io.kestra.plugin.microsoft.fabric.data.engineering;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.models.WorkerJobLifecycle;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.microsoft.fabric.AbstractFabricConnection;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract class AbstractEngineering extends AbstractFabricConnection implements WorkerJobLifecycle {

    private static final String FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1";
    private static final Duration THROTTLED_BACKOFF = Duration.ofSeconds(30);
    static final String STATUS_SUCCEEDED = "Completed";
    static final List<String> TERMINAL_FAILED = List.of("Failed", "Cancelled", "Deduped");

    @Schema(title = "Workspace ID", description = "Microsoft Fabric workspace GUID")
    @jakarta.validation.constraints.NotNull
    @PluginProperty(group = "main")
    protected Property<String> workspaceId;

    @Schema(title = "Execution parameters", description = "Key/value parameters passed to the job execution")
    @Builder.Default
    @PluginProperty(group = "main")
    protected Property<Map<String, Object>> parameters = Property.ofValue(new HashMap<>());

    @Schema(title = "Wait for completion", description = "Poll until the job reaches a terminal state; returns the job instance ID immediately when false")
    @Builder.Default
    @PluginProperty(group = "execution")
    protected Property<Boolean> wait = Property.ofValue(Boolean.TRUE);

    @Schema(title = "Poll frequency", description = "Interval between status checks when wait=true")
    @Builder.Default
    @PluginProperty(group = "execution")
    protected Property<Duration> pollFrequency = Property.ofValue(Duration.ofSeconds(5));

    @Schema(title = "Timeout", description = "Maximum time to wait for completion; throws TimeoutException when exceeded")
    @Builder.Default
    @PluginProperty(group = "execution")
    protected Property<Duration> timeout = Property.ofValue(Duration.ofHours(1));

    /** Counted down by {@link #kill()} and {@link #stop()} to release the poll loop from another thread. */
    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final CountDownLatch cancelSignal = new CountDownLatch(1);

    /** Set once the job is submitted, so a kill can cancel the live job instance rather than a stale one. */
    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final AtomicReference<RunningJob> runningJob = new AtomicReference<>();

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final AtomicBoolean remoteCancelIssued = new AtomicBoolean(false);

    protected String fabricApiBase() {
        return FABRIC_API_BASE;
    }

    protected String resolveToken(RunContext runContext) throws Exception {
        return bearerToken(runContext);
    }

    @Override
    public void kill() {
        cancelSignal.countDown();
        cancelRemoteJob();
    }

    /**
     * Server shutdown: release the poll loop but leave the Fabric job running, it is not the user
     * cancelling the execution. Must stay non-blocking.
     */
    @Override
    public void stop() {
        cancelSignal.countDown();
    }

    private void cancelRemoteJob() {
        var job = runningJob.get();
        if (job == null || !remoteCancelIssued.compareAndSet(false, true)) {
            return;
        }

        var logger = job.runContext().logger();
        try (var client = HttpClient.builder().runContext(job.runContext()).configuration(HttpConfiguration.builder().build()).build()) {
            var response = client.request(HttpRequest.builder()
                .uri(URI.create(job.jobInstanceUrl() + "/cancel"))
                .method("POST")
                .addHeader("Authorization", "Bearer " + job.token())
                .build(), String.class);

            int status = response.getStatus().getCode();
            if (status == 202 || status == 200) {
                logger.info("Requested cancellation of Fabric job instance '{}'", job.jobInstanceId());
            } else {
                logger.warn("Cancellation of Fabric job instance '{}' returned HTTP {}: {}", job.jobInstanceId(), status, response.getBody());
            }
        } catch (Exception e) {
            logger.warn("Failed to cancel Fabric job instance '{}', it may still be running", job.jobInstanceId(), e);
        }
    }

    private boolean isCancelled() {
        return cancelSignal.getCount() == 0;
    }

    /** @return true when the cancel signal fired during the wait. */
    private boolean awaitCancelFor(Duration duration) throws InterruptedException {
        return cancelSignal.await(duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    private InterruptedException cancelled(String jobType, String jobInstanceId, String lastStatus) {
        return new InterruptedException(jobType + " job '" + jobInstanceId + "' polling was cancelled, last status: " + lastStatus);
    }

    protected JobResult submitAndWait(RunContext runContext, String itemId, String jobType) throws Exception {
        var logger = runContext.logger();

        var rWorkspaceId = runContext.render(workspaceId).as(String.class).orElseThrow();
        var rParameters = runContext.render(parameters).asMap(String.class, Object.class);
        var rWait = runContext.render(wait).as(Boolean.class).orElse(Boolean.TRUE);
        var rPollFrequency = runContext.render(pollFrequency).as(Duration.class).orElse(Duration.ofSeconds(5));
        var rTimeout = runContext.render(timeout).as(Duration.class).orElse(Duration.ofHours(1));

        var token = resolveToken(runContext);
        var startUrl = URI.create(fabricApiBase() + "/workspaces/" + rWorkspaceId
            + "/items/" + itemId + "/jobs/instances?jobType=" + jobType);

        var body = Map.of("executionData", Map.of("parameters", rParameters));

        String locationHeader;
        try (var client = HttpClient.builder().runContext(runContext).configuration(HttpConfiguration.builder().build()).build()) {
            var request = HttpRequest.builder()
                .uri(startUrl)
                .method("POST")
                .addHeader("Authorization", "Bearer " + token)
                .body(HttpRequest.JsonRequestBody.builder().content(body).build())
                .build();

            var response = client.request(request, String.class);
            int status = response.getStatus().getCode();
            if (status != 202) {
                throw new RuntimeException("Job submit failed with HTTP " + status + ": " + response.getBody());
            }

            locationHeader = response.getHeaders().firstValue("Location").orElse(null);
            if (locationHeader == null) {
                throw new IllegalStateException("202 response missing Location header");
            }
            logger.info("Submitted {} job for item '{}' in workspace '{}', polling {}", jobType, itemId, rWorkspaceId, locationHeader);
        }

        var jobInstanceId = locationHeader.substring(locationHeader.lastIndexOf('/') + 1);

        // fire-and-forget: the job outlives the task run, so a kill must not cancel it
        if (!Boolean.TRUE.equals(rWait)) {
            return new JobResult(jobInstanceId, "Running");
        }

        runningJob.set(new RunningJob(runContext, locationHeader, jobInstanceId, token));

        // A kill that landed before the job was registered never saw it, so cancel it here.
        if (isCancelled()) {
            cancelRemoteJob();
            throw cancelled(jobType, jobInstanceId, "Running");
        }

        var lastStatus = "Running";
        var deadline = Instant.now().plus(rTimeout);

        while (true) {
            lastStatus = pollStatus(runContext, locationHeader, token);
            logger.debug("{} job '{}' status: {}", jobType, jobInstanceId, lastStatus);

            if (STATUS_SUCCEEDED.equals(lastStatus)) {
                break;
            }
            if (TERMINAL_FAILED.contains(lastStatus)) {
                throw new RuntimeException(jobType + " job ended with status: " + lastStatus);
            }
            if (Instant.now().isAfter(deadline)) {
                throw new TimeoutException(jobType + " job '" + jobInstanceId + "' did not complete within " + rTimeout + "; last status: " + lastStatus);
            }
            if (awaitCancelFor(rPollFrequency)) {
                throw cancelled(jobType, jobInstanceId, lastStatus);
            }
        }

        logger.info("{} job '{}' completed with status '{}'", jobType, jobInstanceId, lastStatus);
        return new JobResult(jobInstanceId, lastStatus);
    }

    private String pollStatus(RunContext runContext, String locationUrl, String token) throws Exception {
        var mapper = JacksonMapper.ofJson();
        try (var client = HttpClient.builder().runContext(runContext).configuration(HttpConfiguration.builder().build()).build()) {
            while (true) {
                var request = HttpRequest.builder()
                    .uri(URI.create(locationUrl))
                    .method("GET")
                    .addHeader("Authorization", "Bearer " + token)
                    .build();

                var response = client.request(request, String.class);
                if (response.getStatus().getCode() == 429) {
                    if (awaitCancelFor(THROTTLED_BACKOFF)) {
                        throw new InterruptedException("Cancelled while backing off from a throttled Fabric status poll");
                    }
                    continue;
                }
                if (response.getStatus().getCode() != 200) {
                    throw new RuntimeException("Status poll failed with HTTP " + response.getStatus().getCode());
                }
                var jobInstance = mapper.readValue(response.getBody(), JobInstanceResponse.class);
                return jobInstance.getStatus();
            }
        }
    }

    record JobResult(String jobInstanceId, String status) {}

    /** Everything {@link #cancelRemoteJob()} needs to reach the running job from the killing thread. */
    record RunningJob(RunContext runContext, String jobInstanceUrl, String jobInstanceId, String token) {
        @Override
        public String toString() {
            // never let the bearer token reach a log line
            return "RunningJob[" + jobInstanceId + "]";
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    static class JobInstanceResponse {
        private String id;
        private String status;
    }
}
