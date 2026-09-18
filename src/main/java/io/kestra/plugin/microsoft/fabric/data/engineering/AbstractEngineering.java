package io.kestra.plugin.microsoft.fabric.data.engineering;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.kestra.core.exceptions.KilledException;
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
import java.util.concurrent.CompletableFuture;
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

    // Never reset at the start of run(): each retry attempt deserializes a fresh task instance, so a
    // reset would only ever swallow a kill delivered just before run() on this same instance.
    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final AtomicBoolean isCancelled = new AtomicBoolean(false);

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final AtomicBoolean cancelDispatched = new AtomicBoolean(false);

    /** Set once the job instance is known, so a kill cancels the live job rather than a stale one. */
    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final AtomicReference<Runnable> killable = new AtomicReference<>();

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final CountDownLatch cancelSignal = new CountDownLatch(1);

    protected String fabricApiBase() {
        return FABRIC_API_BASE;
    }

    protected String resolveToken(RunContext runContext) throws Exception {
        return bearerToken(runContext);
    }

    @Override
    public void kill() {
        this.cancel();
    }

    /**
     * A graceful worker shutdown finishes the task inside the grace period, so it ends terminally and is never
     * resubmitted. Leaving the Fabric job running here would orphan it for good.
     */
    @Override
    public void stop() {
        this.cancel();
    }

    private void cancel() {
        if (!isCancelled.compareAndSet(false, true)) {
            return;
        }

        // Wakes the poll loop now instead of leaving it asleep for up to pollFrequency.
        cancelSignal.countDown();
        dispatchRemoteCancel();
    }

    /**
     * Sends the cancel request on a background thread: kill() and stop() run on the worker lifecycle thread and
     * must not block. Delivery is best-effort, an abrupt worker exit can cut the request off before it lands.
     */
    private void dispatchRemoteCancel() {
        var remoteCancel = killable.get();

        // Nothing to cancel yet: leave cancelDispatched unset, or the dispatch from submitAndWait() once the
        // job instance is known would be silently skipped.
        if (remoteCancel == null || !cancelDispatched.compareAndSet(false, true)) {
            return;
        }

        CompletableFuture.runAsync(remoteCancel);
    }

    private void cancelJobInstance(RunContext runContext, String jobInstanceUrl, String jobInstanceId, String token) {
        var logger = runContext.logger();
        try (var client = HttpClient.builder().runContext(runContext).configuration(HttpConfiguration.builder().build()).build()) {
            var response = client.request(HttpRequest.builder()
                .uri(URI.create(jobInstanceUrl + "/cancel"))
                .method("POST")
                .addHeader("Authorization", "Bearer " + token)
                .build(), String.class);

            int status = response.getStatus().getCode();
            if (status == 200 || status == 202) {
                logger.info("Requested cancellation of Fabric job instance '{}'", jobInstanceId);
            } else {
                logger.warn("Cancellation of Fabric job instance '{}' returned HTTP {}: {}", jobInstanceId, status, response.getBody());
            }
        } catch (Exception e) {
            logger.warn("Failed to cancel Fabric job instance '{}', it may still be running", jobInstanceId, e);
        }
    }

    /** Sleeps for {@code duration}, waking early when a kill or stop lands. */
    private void awaitCancellable(Duration duration) throws InterruptedException {
        try {
            cancelSignal.await(duration.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    protected JobResult submitAndWait(RunContext runContext, String itemId, String jobType) throws Exception {
        var logger = runContext.logger();

        var rWorkspaceId = runContext.render(workspaceId).as(String.class).orElseThrow();
        var rParameters = runContext.render(parameters).asMap(String.class, Object.class);
        var rWait = runContext.render(wait).as(Boolean.class).orElse(Boolean.TRUE);
        var rPollFrequency = runContext.render(pollFrequency).as(Duration.class).orElse(Duration.ofSeconds(5));
        var rTimeout = runContext.render(timeout).as(Duration.class).orElse(Duration.ofHours(1));

        if (isCancelled.get()) {
            throw new KilledException("Task was killed before the " + jobType + " job was submitted");
        }

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

        // fire-and-forget: the job is meant to outlive the task run, so a kill must not cancel it
        if (!Boolean.TRUE.equals(rWait)) {
            return new JobResult(jobInstanceId, "Running");
        }

        killable.set(() -> cancelJobInstance(runContext, locationHeader, jobInstanceId, token));

        // A kill landing between the submit and the line above found nothing to cancel, so dispatch it here.
        if (isCancelled.get()) {
            dispatchRemoteCancel();
            throw new KilledException(jobType + " job '" + jobInstanceId + "' was killed before completion");
        }

        var lastStatus = "Running";
        var deadline = Instant.now().plus(rTimeout);

        while (true) {
            if (isCancelled.get()) {
                throw new KilledException("Stopped polling " + jobType + " job '" + jobInstanceId
                    + "': the task was killed or the worker is shutting down");
            }

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

            awaitCancellable(rPollFrequency);
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
                    awaitCancellable(THROTTLED_BACKOFF);
                    if (isCancelled.get()) {
                        throw new KilledException("Stopped polling Fabric job instance: throttled, then the task was killed or the worker is shutting down");
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

    @JsonIgnoreProperties(ignoreUnknown = true)
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    static class JobInstanceResponse {
        private String id;
        private String status;
    }
}
