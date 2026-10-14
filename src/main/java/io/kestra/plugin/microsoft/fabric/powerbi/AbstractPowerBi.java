package io.kestra.plugin.microsoft.fabric.powerbi;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientException;
import io.kestra.core.http.client.HttpClientRequestException;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractPowerBi extends Task {
    private static final String LOGIN_URL = "https://login.microsoftonline.com";
    private static final String API_URL = "https://api.powerbi.com";

    @NotNull
    @NotEmpty
    @Schema(title = "Azure tenant ID")
    @PluginProperty(dynamic = true, group = "main")
    private String tenantId;

    @NotNull
    @NotEmpty
    @Schema(title = "Azure client ID")
    @PluginProperty(dynamic = true, group = "main")
    private String clientId;

    @NotNull
    @NotEmpty
    @Schema(title = "Azure client secret")
    @PluginProperty(dynamic = true, group = "main", secret = true)
    @ToString.Exclude
    private String clientSecret;

    @Schema(title = "The HTTP client configuration")
    @PluginProperty(group = "advanced")
    protected HttpConfiguration options;

    @Getter(AccessLevel.NONE)
    private transient String token;

    /** Override points allow tests to exercise the same auth flow against WireMock. */
    protected String loginUrl() {
        return LOGIN_URL;
    }

    protected String apiUrl() {
        return API_URL;
    }

    private String token(RunContext runContext) throws IllegalVariableEvaluationException {
        if (this.token != null) {
            return this.token;
        }

        var renderedTenantId = runContext.render(this.tenantId);
        var renderedClientId = runContext.render(this.clientId);
        var renderedClientSecret = runContext.render(this.clientSecret);
        URI uri = URI.create(loginUrl() + "/" + renderedTenantId + "/oauth2/token");

        HttpRequest request = HttpRequest.builder()
            .uri(uri)
            .method("POST")
            .body(
                HttpRequest.UrlEncodedRequestBody.builder()
                    .content(
                        Map.of(
                            "grant_type", "client_credentials",
                            "client_id", renderedClientId,
                            "client_secret", renderedClientSecret,
                            "resource", "https://analysis.windows.net/powerbi/api",
                            "scope", "https://analysis.windows.net/powerbi/api/.default"
                        )
                    )
                    .build()
            )
            .addHeader("Content-Type", "application/x-www-form-urlencoded")
            .build();

        try (HttpClient client = new HttpClient(runContext, httpConfiguration())) {
            HttpResponse<String> exchange = client.request(request, String.class);
            Map<String, String> tokenResponse = JacksonMapper.ofJson().readValue(exchange.getBody(), new TypeReference<>() {});
            if (tokenResponse == null || tokenResponse.get("access_token") == null || tokenResponse.get("access_token").isBlank()) {
                throw new IllegalStateException("Invalid token request response: missing access_token");
            }
            this.token = tokenResponse.get("access_token");
            return this.token;
        } catch (HttpClientRequestException | HttpClientResponseException e) {
            throw new RuntimeException("Failed to fetch Power BI access token", e);
        } catch (HttpClientException e) {
            throw new RuntimeException("Power BI HTTP client failed", e);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse Power BI token response", e);
        }
    }

    protected <REQ, RES> HttpResponse<RES> request(
        RunContext runContext,
        HttpRequest request,
        Class<RES> responseType
    ) throws HttpClientException, IllegalVariableEvaluationException {
        var authorizedRequest = HttpRequest.builder()
            .uri(request.getUri())
            .method(request.getMethod())
            .body(request.getBody())
            .addHeader("Authorization", "Bearer " + this.token(runContext))
            .addHeader("Content-Type", "application/json")
            .build();

        try (HttpClient client = new HttpClient(runContext, responseConfiguration())) {
            return client.request(authorizedRequest, responseType);
        } catch (IOException | IllegalVariableEvaluationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Return HTTP error responses to the task so it can apply operation-specific handling. The
     * normal request path fails on 4xx/5xx before exposing the status, which would make it
     * impossible to handle Power BI HTTP 429 while polling. Status validation is therefore
     * owned by this task rather than the generic client's allowed-response-code configuration.
     */
    private HttpConfiguration responseConfiguration() {
        return httpConfiguration().toBuilder()
            .allowFailed(Property.ofValue(true))
            .allowedResponseCodes(null)
            .build();
    }

    /** Power BI uses its own OAuth token; do not attach the HTTP client's default Basic auth. */
    private HttpConfiguration httpConfiguration() {
        HttpConfiguration base = options == null ? HttpConfiguration.builder().build() : options;
        return base.toBuilder().auth(null).build();
    }

}