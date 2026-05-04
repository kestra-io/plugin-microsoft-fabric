package io.kestra.plugin.microsoft.fabric.powerbi;

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
                id: fabric_powerbi_refresh
                namespace: company.team

                tasks:
                  - id: refresh_dataset
                    type: io.kestra.plugin.microsoft.fabric.powerbi.RefreshDataset
                    tenantId: "{{ secret('FABRIC_TENANT_ID') }}"
                    clientId: "{{ secret('FABRIC_CLIENT_ID') }}"
                    clientSecret: "{{ secret('FABRIC_CLIENT_SECRET') }}"
                    workspaceId: "your-workspace-id"
                    datasetId: "your-semantic-model-id"
                    notifyOption: NoNotification
                """
        )
    }
)
@Schema(
    title = "Refresh a Power BI Semantic Model (Dataset)",
    description = """
        Triggers a Power BI semantic model refresh via the Fabric REST API and optionally waits for completion.
        Uses the `/workspaces/{workspaceId}/semanticModels/{datasetId}/refreshes` endpoint.
        Defaults: wait=true, pollFrequency=PT10S, timeout=PT1H.
        """
)
public class RefreshDataset extends AbstractFabricConnection implements RunnableTask<RefreshDataset.Output> {

    private static final String FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1";
    private static final String STATUS_COMPLETED = "Completed";
    private static final List<String> TERMINAL_FAILED = List.of("Failed", "Cancelled", "Disabled");

    @Schema(title = "Workspace ID", description = "Microsoft Fabric workspace GUID containing the semantic model")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> workspaceId;

    @Schema(title = "Dataset ID", description = "Power BI semantic model (dataset) GUID")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> datasetId;

    @Schema(
        title = "Notify option",
        description = "Email notification setting: NoNotification, MailOnFailure, or MailOnCompletion"
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<NotifyOption> notifyOption = Property.ofValue(NotifyOption.NoNotification);

    @Schema(title = "Wait for completion", description = "Poll until the refresh reaches a terminal state; returns refreshId immediately when false")
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Boolean> wait = Property.ofValue(Boolean.TRUE);

    @Schema(title = "Poll frequency", description = "Interval between status checks when wait=true")
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Duration> pollFrequency = Property.ofValue(Duration.ofSeconds(10));

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
        var mapper = JacksonMapper.ofJson();

        var rWorkspaceId = runContext.render(workspaceId).as(String.class).orElseThrow();
        var rDatasetId = runContext.render(datasetId).as(String.class).orElseThrow();
        var rNotifyOption = runContext.render(notifyOption).as(NotifyOption.class).orElse(NotifyOption.NoNotification);
        var rWait = runContext.render(wait).as(Boolean.class).orElse(Boolean.TRUE);
        var rPollFrequency = runContext.render(pollFrequency).as(Duration.class).orElse(Duration.ofSeconds(10));
        var rTimeout = runContext.render(timeout).as(Duration.class).orElse(Duration.ofHours(1));

        var token = resolveToken(runContext);
        var refreshUrl = URI.create(fabricApiBase() + "/workspaces/" + rWorkspaceId
            + "/semanticModels/" + rDatasetId + "/refreshes");

        var body = new HashMap<String, Object>();
        body.put("notifyOption", rNotifyOption.name());

        String refreshId;
        try (var client = HttpClient.builder().runContext(runContext).configuration(HttpConfiguration.builder().build()).build()) {
            var request = HttpRequest.builder()
                .uri(refreshUrl)
                .method("POST")
                .addHeader("Authorization", "Bearer " + token)
                .body(HttpRequest.JsonRequestBody.builder().content(body).build())
                .build();

            var response = client.request(request, String.class);
            int status = response.getStatus().getCode();
            if (status != 202) {
                throw new RuntimeException("Dataset refresh start failed with HTTP " + status + ": " + response.getBody());
            }

            var locationHeader = response.getHeaders().firstValue("Location").orElse(null);
            if (locationHeader == null) {
                throw new IllegalStateException("202 response missing Location header");
            }

            refreshId = locationHeader.substring(locationHeader.lastIndexOf('/') + 1);
            logger.info("Started refresh '{}' for dataset '{}' in workspace '{}'", refreshId, rDatasetId, rWorkspaceId);
        }

        if (!Boolean.TRUE.equals(rWait)) {
            return Output.builder().refreshId(refreshId).status("Running").build();
        }

        var pollUrl = fabricApiBase() + "/workspaces/" + rWorkspaceId
            + "/semanticModels/" + rDatasetId + "/refreshes/" + refreshId;

        var lastStatus = new AtomicReference<>("Running");
        try {
            Await.until(() -> {
                try {
                    var status = pollRefreshStatus(runContext, pollUrl, token, mapper);
                    lastStatus.set(status);
                    logger.debug("Dataset refresh '{}' status: {}", refreshId, status);
                    if (TERMINAL_FAILED.contains(status)) {
                        throw new RuntimeException("Dataset refresh ended with status: " + status);
                    }
                    return STATUS_COMPLETED.equals(status);
                } catch (RuntimeException re) {
                    throw re;
                } catch (Exception e) {
                    throw new RuntimeException("Error polling refresh status", e);
                }
            }, rPollFrequency, rTimeout);
        } catch (TimeoutException e) {
            throw new TimeoutException("Dataset refresh '" + refreshId + "' did not complete within " + rTimeout + "; last status: " + lastStatus.get());
        }

        logger.info("Dataset refresh '{}' completed with status '{}'", refreshId, lastStatus.get());
        return Output.builder().refreshId(refreshId).status(lastStatus.get()).build();
    }

    private String pollRefreshStatus(RunContext runContext, String pollUrl, String token,
                                     com.fasterxml.jackson.databind.ObjectMapper mapper) throws Exception {
        try (var client = HttpClient.builder().runContext(runContext).configuration(HttpConfiguration.builder().build()).build()) {
            var request = HttpRequest.builder()
                .uri(URI.create(pollUrl))
                .method("GET")
                .addHeader("Authorization", "Bearer " + token)
                .build();

            var response = client.request(request, String.class);
            if (response.getStatus().getCode() == 429) {
                Thread.sleep(30_000);
                return pollRefreshStatus(runContext, pollUrl, token, mapper);
            }
            if (response.getStatus().getCode() != 200) {
                throw new RuntimeException("Status poll failed with HTTP " + response.getStatus().getCode());
            }
            var refreshResponse = mapper.readValue(response.getBody(), RefreshResponse.class);
            return refreshResponse.getStatus();
        }
    }

    public enum NotifyOption {
        NoNotification,
        MailOnFailure,
        MailOnCompletion
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Refresh ID", description = "Unique ID of the dataset refresh operation")
        private final String refreshId;

        @Schema(title = "Final status", description = "Terminal status of the refresh operation")
        private final String status;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    static class RefreshResponse {
        private String refreshId;
        private String status;
    }
}
