package io.kestra.plugin.microsoft.fabric.engineering;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.Await;
import io.kestra.plugin.microsoft.fabric.AbstractFabricConnection;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract class AbstractEngineering extends AbstractFabricConnection {

    private static final String FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1";
    static final String STATUS_SUCCEEDED = "Succeeded";
    static final List<String> TERMINAL_FAILED = List.of("Failed", "Cancelled", "Deduped");

    @Schema(title = "Workspace ID", description = "Microsoft Fabric workspace GUID")
    @jakarta.validation.constraints.NotNull
    @PluginProperty(group = "main")
    protected Property<String> workspaceId;

    @Schema(title = "Execution parameters", description = "Key/value parameters passed to the job execution")
    @Builder.Default
    @PluginProperty(group = "main")
    protected Property<Map<String, Object>> parameters = Property.ofValue(new HashMap<>());

    @Schema(title = "Wait for completion", description = "Poll until the job reaches a terminal state; returns jobInstanceId immediately when false")
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

    protected String fabricApiBase() {
        return FABRIC_API_BASE;
    }

    protected String resolveToken(RunContext runContext) throws Exception {
        return bearerToken(runContext);
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

        if (!Boolean.TRUE.equals(rWait)) {
            return new JobResult(jobInstanceId, "Running");
        }

        var lastStatus = new AtomicReference<>("Running");
        try {
            Await.until(() -> {
                try {
                    var status = pollStatus(runContext, locationHeader, token);
                    lastStatus.set(status);
                    logger.debug("{} job '{}' status: {}", jobType, jobInstanceId, status);
                    if (TERMINAL_FAILED.contains(status)) {
                        throw new RuntimeException(jobType + " job ended with status: " + status);
                    }
                    return STATUS_SUCCEEDED.equals(status);
                } catch (RuntimeException re) {
                    throw re;
                } catch (Exception e) {
                    throw new RuntimeException("Error polling job status", e);
                }
            }, rPollFrequency, rTimeout);
        } catch (TimeoutException e) {
            throw new TimeoutException(jobType + " job '" + jobInstanceId + "' did not complete within " + rTimeout + "; last status: " + lastStatus.get());
        }

        logger.info("{} job '{}' completed with status '{}'", jobType, jobInstanceId, lastStatus.get());
        return new JobResult(jobInstanceId, lastStatus.get());
    }

    private String pollStatus(RunContext runContext, String locationUrl, String token) throws Exception {
        var mapper = JacksonMapper.ofJson();
        try (var client = HttpClient.builder().runContext(runContext).configuration(HttpConfiguration.builder().build()).build()) {
            var request = HttpRequest.builder()
                .uri(URI.create(locationUrl))
                .method("GET")
                .addHeader("Authorization", "Bearer " + token)
                .build();

            var response = client.request(request, String.class);
            if (response.getStatus().getCode() == 429) {
                Thread.sleep(30_000);
                return pollStatus(runContext, locationUrl, token);
            }
            if (response.getStatus().getCode() != 200) {
                throw new RuntimeException("Status poll failed with HTTP " + response.getStatus().getCode());
            }
            var jobInstance = mapper.readValue(response.getBody(), JobInstanceResponse.class);
            return jobInstance.getStatus();
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
