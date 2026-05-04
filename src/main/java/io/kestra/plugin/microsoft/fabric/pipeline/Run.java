package io.kestra.plugin.microsoft.fabric.pipeline;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.Await;
import io.kestra.plugin.microsoft.fabric.AbstractFabricConnection;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
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
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: fabric_pipeline_run
                namespace: company.team

                tasks:
                  - id: run_pipeline
                    type: io.kestra.plugin.microsoft.fabric.pipeline.Run
                    tenantId: "{{ secret('FABRIC_TENANT_ID') }}"
                    clientId: "{{ secret('FABRIC_CLIENT_ID') }}"
                    clientSecret: "{{ secret('FABRIC_CLIENT_SECRET') }}"
                    workspaceId: "your-workspace-id"
                    pipelineId: "your-pipeline-id"
                    parameters:
                      myParam: "myValue"
                """
        )
    }
)
@Schema(
    title = "Run a Microsoft Fabric Data Pipeline",
    description = """
        Triggers a Fabric Data Pipeline job and optionally waits for completion.
        Posts to the Fabric scheduler API with `jobType=Pipeline`, polls the returned job instance URL,
        and fails if the pipeline ends in `Failed`, `Cancelled`, or `Deduped` state.
        Defaults: wait=true, pollFrequency=PT5S, timeout=PT1H.
        """
)
public class Run extends AbstractFabricConnection implements RunnableTask<Run.Output> {

    private static final String FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1";
    private static final String STATUS_SUCCEEDED = "Succeeded";
    private static final List<String> TERMINAL_FAILED = List.of("Failed", "Cancelled", "Deduped");

    @Schema(title = "Workspace ID", description = "Microsoft Fabric workspace GUID")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> workspaceId;

    @Schema(title = "Pipeline ID", description = "Microsoft Fabric Data Pipeline item GUID")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> pipelineId;

    @Schema(title = "Pipeline parameters", description = "Key/value parameters passed to the pipeline execution")
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<Map<String, Object>> parameters = Property.ofValue(new HashMap<>());

    @Schema(title = "Wait for completion", description = "Poll until the pipeline job reaches a terminal state; returns runId immediately when false")
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Boolean> wait = Property.ofValue(Boolean.TRUE);

    @Schema(title = "Poll frequency", description = "Interval between status checks when wait=true")
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Duration> pollFrequency = Property.ofValue(Duration.ofSeconds(5));

    @Schema(title = "Timeout", description = "Maximum time to wait for completion; throws TimeoutException when exceeded")
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Duration> timeout = Property.ofValue(Duration.ofHours(1));

    protected String fabricApiBase() {
        return FABRIC_API_BASE;
    }

    protected String resolveToken(RunContext runContext) throws Exception {
        return bearerToken(runContext);
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();

        var rWorkspaceId = runContext.render(workspaceId).as(String.class).orElseThrow();
        var rPipelineId = runContext.render(pipelineId).as(String.class).orElseThrow();
        var rParameters = runContext.render(parameters).asMap(String.class, Object.class);
        var rWait = runContext.render(wait).as(Boolean.class).orElse(Boolean.TRUE);
        var rPollFrequency = runContext.render(pollFrequency).as(Duration.class).orElse(Duration.ofSeconds(5));
        var rTimeout = runContext.render(timeout).as(Duration.class).orElse(Duration.ofHours(1));

        var token = resolveToken(runContext);
        var startUrl = URI.create(fabricApiBase() + "/workspaces/" + rWorkspaceId
            + "/items/" + rPipelineId + "/jobs/instances?jobType=Pipeline");

        var body = Map.of("executionData", Map.of("parameters", rParameters));

        String locationHeader;
        try (var client = HttpClient.builder().runContext(runContext).configuration(HttpConfiguration.builder().build()).build()) {
            var request = HttpRequest.builder()
                .uri(startUrl)
                .method("POST")
                .addHeader("Authorization", "Bearer " + token)
                .body(HttpRequest.JsonRequestBody.builder()
                    .content(body)
                    .build())
                .build();

            var response = client.request(request, String.class);
            int status = response.getStatus().getCode();
            if (status != 202) {
                throw new RuntimeException("Pipeline run start failed with HTTP " + status + ": " + response.getBody());
            }

            locationHeader = response.getHeaders().firstValue("Location").orElse(null);
            if (locationHeader == null) {
                throw new IllegalStateException("202 response missing Location header");
            }
            logger.info("Started pipeline '{}' in workspace '{}', polling {}", rPipelineId, rWorkspaceId, locationHeader);
        }

        var runId = locationHeader.substring(locationHeader.lastIndexOf('/') + 1);

        if (!Boolean.TRUE.equals(rWait)) {
            return Output.builder().runId(runId).status("Running").build();
        }

        var lastStatus = new AtomicReference<>("Running");
        try {
            Await.until(() -> {
                try {
                    var status = pollJobStatus(runContext, locationHeader, token);
                    lastStatus.set(status);
                    logger.debug("Pipeline job '{}' status: {}", runId, status);
                    if (TERMINAL_FAILED.contains(status)) {
                        throw new RuntimeException("Pipeline job ended with status: " + status);
                    }
                    return STATUS_SUCCEEDED.equals(status);
                } catch (RuntimeException re) {
                    throw re;
                } catch (Exception e) {
                    throw new RuntimeException("Error polling job status", e);
                }
            }, rPollFrequency, rTimeout);
        } catch (TimeoutException e) {
            throw new TimeoutException("Pipeline job '" + runId + "' did not complete within " + rTimeout + "; last status: " + lastStatus.get());
        }

        logger.info("Pipeline job '{}' completed with status '{}'", runId, lastStatus.get());
        return Output.builder().runId(runId).status(lastStatus.get()).build();
    }

    private String pollJobStatus(RunContext runContext, String locationUrl, String token) throws Exception {
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
                return pollJobStatus(runContext, locationUrl, token);
            }
            if (response.getStatus().getCode() != 200) {
                throw new RuntimeException("Status poll failed with HTTP " + response.getStatus().getCode());
            }
            var jobInstance = mapper.readValue(response.getBody(), JobInstanceResponse.class);
            return jobInstance.getStatus();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Job instance ID", description = "Unique ID of the pipeline job instance")
        private final String runId;

        @Schema(title = "Final status", description = "Terminal status of the pipeline job: Succeeded, Failed, Cancelled, or Deduped")
        private final String status;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @Getter
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    static class JobInstanceResponse {
        private String id;
        private String status;
    }
}
