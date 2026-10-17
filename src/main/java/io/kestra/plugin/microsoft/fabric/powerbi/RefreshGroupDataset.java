package io.kestra.plugin.microsoft.fabric.powerbi;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.Await;
import io.kestra.plugin.microsoft.fabric.powerbi.models.Refresh;
import io.kestra.plugin.microsoft.fabric.powerbi.models.Refreshes;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static io.kestra.core.utils.Rethrow.throwSupplier;

@SuperBuilder
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@Getter
@NoArgsConstructor
@Schema(
    title = "Refresh a Power BI dataset",
    description = "Starts an asynchronous refresh for a dataset in a workspace. Optionally waits for completion, polling every 5 seconds by default until completion or a 10-minute timeout. Requires a service principal with refresh permissions on the target workspace and dataset."
)
@Plugin(
    aliases = "io.kestra.plugin.powerbi.RefreshGroupDataset",
    examples = {
        @Example(
            full = true,
            title = "Refresh a workspace dataset",
            code = """
                id: refresh-powerbi-dataset
                namespace: company.team

                tasks:
                  - id: refresh_sales_model
                    type: io.kestra.plugin.microsoft.fabric.powerbi.RefreshGroupDataset
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    groupId: "9f1c2b8c-12ab-4f3e-9bcd-0af2c5e6d123"
                    datasetId: "5c8b4a1e-7f89-4c33-9dd8-1f23c4567890"
                    wait: true
                    pollDuration: "PT5S"
                    waitDuration: "PT10M"
                """
        )
    }
)
public class RefreshGroupDataset extends AbstractPowerBi implements RunnableTask<RefreshGroupDataset.Output> {
    @NotNull
    @Schema(title = "Workspace (group) ID", description = "GUID of the workspace containing the dataset.")
    @PluginProperty(group = "main")
    private Property<String> groupId;

    @NotNull
    @Schema(title = "Dataset ID", description = "GUID of the dataset to refresh.")
    @PluginProperty(group = "main")
    private Property<String> datasetId;

    @Schema(
        title = "Wait for refresh completion",
        description = "Defaults to `false`. When `true`, polls the refresh status and fails if the final status is not `Completed`."
    )
    @NotNull
    @lombok.Builder.Default
    @PluginProperty(group = "execution")
    private Property<Boolean> wait = Property.ofValue(false);

    @Schema(title = "Polling interval", description = "Delay between status checks when `wait` is true; defaults to 5 seconds.")
    @NotNull
    @lombok.Builder.Default
    @PluginProperty(group = "execution")
    private final Property<Duration> pollDuration = Property.ofValue(Duration.ofSeconds(5));

    @Schema(title = "Refresh wait timeout", description = "Maximum time to wait for completion when `wait` is true; defaults to 10 minutes.")
    @NotNull
    @lombok.Builder.Default
    @PluginProperty(group = "execution")
    private final Property<Duration> waitDuration = Property.ofValue(Duration.ofMinutes(10));

    @Schema(
        title = "Refresh notification option",
        description = "Optional notification behavior. If omitted, no notification option is sent, preserving the legacy Power BI API request."
    )
    @PluginProperty(group = "advanced")
    private Property<NotifyOption> notifyOption;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        var renderedGroupId = runContext.render(groupId).as(String.class).orElseThrow();
        var renderedDatasetId = runContext.render(datasetId).as(String.class).orElseThrow();
        var refreshesUrl = apiUrl() + "/v1.0/myorg/groups/" + encodePathSegment(renderedGroupId)
            + "/datasets/" + encodePathSegment(renderedDatasetId) + "/refreshes";

        var postRequestBuilder = HttpRequest.builder()
            .uri(URI.create(refreshesUrl))
            .method("POST");
        if (notifyOption != null) {
            var renderedNotifyOption = runContext.render(notifyOption).as(NotifyOption.class).orElse(null);
            if (renderedNotifyOption != null) {
                var body = new HashMap<String, Object>();
                body.put("notifyOption", renderedNotifyOption.name());
                postRequestBuilder.body(HttpRequest.JsonRequestBody.builder().content(body).build());
            }
        }

        var accessToken = fetchToken(runContext);
        HttpResponse<String> response = request(runContext, accessToken, postRequestBuilder.build(), String.class);
        if (response.getStatus().getCode() < 200 || response.getStatus().getCode() >= 300) {
            throw new IllegalStateException("Power BI refresh request failed with HTTP " + response.getStatus().getCode() + ": " + response.getBody());
        }

        // Keep the legacy header first, but also accept the documented Power BI async-operation
        // headers. The Location resource identifies the refresh itself; x-ms-request-id is the
        // fallback when the service does not return a Location header.
        Optional<String> refreshId = response.getHeaders().firstValue("requestId")
            .filter(id -> !id.isBlank())
            .or(() -> response.getHeaders().firstValue("Location").flatMap(RefreshGroupDataset::requestIdFromLocation))
            .or(() -> response.getHeaders().firstValue("x-ms-request-id").filter(id -> !id.isBlank()));
        if (refreshId.isEmpty() || refreshId.get().isBlank()) {
            throw new IllegalStateException(
                "Invalid Power BI refresh response: missing requestId header, body '" + response.getBody() + "', headers '" + response.getHeaders() + "'"
            );
        }

        logger.info("Power BI refresh created with request id '{}'", refreshId.get());
        if (!runContext.render(wait).as(Boolean.class).orElse(false)) {
            return Output.builder().requestId(refreshId.get()).build();
        }

        Duration renderedPollDuration = runContext.render(pollDuration).as(Duration.class).orElse(Duration.ofSeconds(5));
        Duration renderedWaitDuration = runContext.render(waitDuration).as(Duration.class).orElse(Duration.ofMinutes(10));
        Refresh result;
        try {
            result = Await.until(
                throwSupplier(() -> pollRefresh(runContext, accessToken, refreshesUrl, refreshId.get(), loggerTraceEnabled(runContext))),
                renderedPollDuration,
                renderedWaitDuration
            );
        } catch (TimeoutException e) {
            throw new TimeoutException("Power BI refresh '" + refreshId.get() + "' did not complete within " + renderedWaitDuration);
        }

        if (result.status() == null || !"completed".equals(result.status().toLowerCase(Locale.ROOT))) {
            throw new Exception("Power BI refresh failed with status '" + result.status() + "' with response " + result);
        }

        return Output.builder()
            .requestId(refreshId.get())
            .status(result.status())
            .extendedStatus(result.extendedStatus())
            .refreshType(result.refreshType())
            .startTime(result.startTime())
            .endTime(result.endTime())
            .build();
    }

    private static Optional<String> requestIdFromLocation(String location) {
        try {
            String path = URI.create(location).getRawPath();
            if (path == null) {
                return Optional.empty();
            }

            String[] segments = path.split("/");
            if (segments.length < 2
                || !"refreshes".equals(segments[segments.length - 2])
                || segments[segments.length - 1].isBlank()) {
                return Optional.empty();
            }

            return Optional.of(segments[segments.length - 1]);
        } catch (IllegalArgumentException e) {
            // Ignore malformed Location headers and continue to the x-ms-request-id fallback.
            return Optional.empty();
        }
    }

    private boolean loggerTraceEnabled(RunContext runContext) {
        return runContext.logger().isTraceEnabled();
    }

    private Refresh pollRefresh(RunContext runContext, String accessToken, String refreshesUrl, String refreshId, boolean traceEnabled)
        throws Exception {
        HttpRequest getRequest = HttpRequest.builder().uri(URI.create(refreshesUrl)).method("GET").build();
        // Read as text first so non-JSON throttling/error bodies cannot fail deserialization before we
        // inspect the HTTP status.
        HttpResponse<String> response = request(runContext, accessToken, getRequest, String.class);
        int statusCode = response.getStatus().getCode();

        // A throttled poll did not return refresh state. Let Await apply the configured interval and timeout.
        if (statusCode == 429) {
            runContext.logger().warn("Power BI throttled refresh-status polling; retrying after the configured poll interval");
            return null;
        }
        if (statusCode < 200 || statusCode >= 300) {
            throw new IllegalStateException("Power BI refresh-status polling failed with HTTP " + statusCode + ": " + response.getBody());
        }

        Refreshes refreshes = response.getBody() == null || response.getBody().isBlank()
            ? null
            : JacksonMapper.ofJson().readValue(response.getBody(), Refreshes.class);
        Optional<Refresh> refresh = Optional.ofNullable(refreshes)
            .map(Refreshes::value)
            .stream()
            .flatMap(List::stream)
            .filter(item -> refreshId.equals(item.requestId()))
            .findFirst();

        if (refresh.isEmpty()) {
            runContext.logger().debug("Power BI refresh '{}' is not yet present in history; waiting", refreshId);
            return null;
        }

        Refresh current = refresh.get();
        if (traceEnabled) {
            runContext.logger().trace("Power BI refresh: {}", current);
        }
        if (current.status() == null || "Unknown".equalsIgnoreCase(current.status())) {
            return null;
        }
        return current;
    }

    public enum NotifyOption {
        NoNotification,
        MailOnFailure,
        MailOnCompletion
    }

    @lombok.Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Refresh request ID", description = "Always returned, even when `wait` is `false`.")
        private String requestId;

        @Schema(title = "Refresh status", description = "Returned only when `wait` is `true`; task fails if the final status is not `Completed`.")
        private String status;

        @Schema(title = "Refresh extended status", description = "Returned only when `wait` is `true`.")
        private String extendedStatus;

        @Schema(title = "Refresh type", description = "Returned only when `wait` is `true`.")
        private String refreshType;

        @Schema(title = "Refresh start time", description = "Returned only when `wait` is `true`.")
        private Instant startTime;

        @Schema(title = "Refresh end time", description = "Returned only when `wait` is `true`.")
        private Instant endTime;
    }
}