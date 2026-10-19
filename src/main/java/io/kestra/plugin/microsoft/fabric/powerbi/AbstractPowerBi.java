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
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractPowerBi extends Task {
    private static final String DEFAULT_LOGIN_URL = "https://login.microsoftonline.com";
    private static final String DEFAULT_API_URL = "https://api.powerbi.com";
    private static final String DEFAULT_OAUTH_RESOURCE = "https://analysis.windows.net/powerbi/api";
    private static final String DEFAULT_OAUTH_SCOPE = "https://analysis.windows.net/powerbi/api/.default";

    @NotNull
    @Schema(title = "Azure tenant ID")
    @PluginProperty(group = "main")
    private Property<String> tenantId;

    @NotNull
    @Schema(title = "Azure client ID")
    @PluginProperty(group = "main")
    private Property<String> clientId;

    @NotNull
    @Schema(title = "Azure client secret")
    @PluginProperty(group = "main", secret = true)
    @ToString.Exclude
    private Property<String> clientSecret;

    @NotNull
    @Schema(title = "Microsoft identity endpoint", description = "Base URL for OAuth token requests. Defaults to https://login.microsoftonline.com.")
    @PluginProperty(group = "connection")
    @Builder.Default
    private Property<String> loginUrl = Property.ofValue(DEFAULT_LOGIN_URL);

    @NotNull
    @Schema(title = "Power BI API endpoint", description = "Base URL for Power BI REST requests. Defaults to https://api.powerbi.com.")
    @PluginProperty(group = "connection")
    @Builder.Default
    private Property<String> apiUrl = Property.ofValue(DEFAULT_API_URL);

    @NotNull
    @Schema(title = "Power BI OAuth resource", description = "Resource requested from Microsoft identity. Defaults to the public-cloud Power BI resource.")
    @PluginProperty(group = "connection")
    @Builder.Default
    private Property<String> oauthResource = Property.ofValue(DEFAULT_OAUTH_RESOURCE);

    @NotNull
    @Schema(title = "Power BI OAuth scope", description = "Scope requested from Microsoft identity. Defaults to the public-cloud Power BI scope.")
    @PluginProperty(group = "connection")
    @Builder.Default
    private Property<String> oauthScope = Property.ofValue(DEFAULT_OAUTH_SCOPE);

    @Schema(title = "The HTTP client configuration")
    @PluginProperty(group = "advanced")
    protected HttpConfiguration options;

    // Encode dots as well so literal "." and ".." cannot become traversal segments.
    protected static String encodePathSegment(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8)
            .replace("+", "%20")
            .replace(".", "%2E");
    }

    String loginBaseUrl(RunContext runContext) throws IllegalVariableEvaluationException {
        return renderBaseUrl(runContext, loginUrl, DEFAULT_LOGIN_URL, "loginUrl");
    }

    String apiBaseUrl(RunContext runContext) throws IllegalVariableEvaluationException {
        return renderBaseUrl(runContext, apiUrl, DEFAULT_API_URL, "apiUrl");
    }

    String fetchToken(RunContext runContext) throws IllegalVariableEvaluationException {
        var rTenantId = runContext.render(this.tenantId).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("tenantId is required"));
        var rClientId = runContext.render(this.clientId).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("clientId is required"));
        var rClientSecret = runContext.render(this.clientSecret).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("clientSecret is required"));
        var rOAuthResource = runContext.render(oauthResource).as(String.class).orElse(DEFAULT_OAUTH_RESOURCE);
        var rOAuthScope = runContext.render(oauthScope).as(String.class).orElse(DEFAULT_OAUTH_SCOPE);
        var rLoginUrl = loginBaseUrl(runContext);
        var uri = URI.create(rLoginUrl + "/" + encodePathSegment(rTenantId) + "/oauth2/token");

        var request = HttpRequest.builder()
            .uri(uri)
            .method("POST")
            .body(
                HttpRequest.UrlEncodedRequestBody.builder()
                    .content(
                        Map.of(
                            "grant_type", "client_credentials",
                            "client_id", rClientId,
                            "client_secret", rClientSecret,
                            "resource", rOAuthResource,
                            "scope", rOAuthScope
                        )
                    )
                    .build()
            )
            .addHeader("Content-Type", "application/x-www-form-urlencoded")
            .build();

        try (HttpClient client = new HttpClient(runContext, httpConfiguration())) {
            var exchange = client.request(request, String.class);
            Map<String, String> tokenResponse = JacksonMapper.ofJson().readValue(exchange.getBody(), new TypeReference<>() {});
            if (tokenResponse == null || tokenResponse.get("access_token") == null || tokenResponse.get("access_token").isBlank()) {
                throw new IllegalStateException("Invalid token request response: missing access_token");
            }
            return tokenResponse.get("access_token");
        } catch (HttpClientRequestException | HttpClientResponseException e) {
            throw new RuntimeException("Failed to fetch Power BI access token", e);
        } catch (HttpClientException e) {
            throw new RuntimeException("Power BI HTTP client failed", e);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse Power BI token response", e);
        }
    }

    <RES> HttpResponse<RES> request(
        RunContext runContext,
        String accessToken,
        HttpRequest request,
        Class<RES> responseType
    ) throws HttpClientException, IllegalVariableEvaluationException {
        var authorizedRequest = HttpRequest.builder()
            .uri(request.getUri())
            .method(request.getMethod())
            .body(request.getBody())
            .addHeader("Authorization", "Bearer " + accessToken)
            .addHeader("Content-Type", "application/json")
            .build();

        try (HttpClient client = new HttpClient(runContext, responseConfiguration())) {
            return client.request(authorizedRequest, responseType);
        } catch (IOException | IllegalVariableEvaluationException e) {
            throw new RuntimeException(e);
        }
    }

    private String renderBaseUrl(RunContext runContext, Property<String> endpoint, String defaultUrl, String propertyName)
        throws IllegalVariableEvaluationException {
        if (endpoint == null) {
            return defaultUrl;
        }
        var rBaseUrl = runContext.render(endpoint).as(String.class)
            .filter(value -> !value.isBlank())
            .orElse(defaultUrl);
        return rBaseUrl.replaceAll("/+$", "");
    }

    private HttpConfiguration responseConfiguration() {
        // allowFailed exposes 401/429 so the task can handle status codes itself.
        return httpConfiguration().toBuilder()
            .allowFailed(Property.ofValue(true))
            .allowedResponseCodes(null)
            .auth(null)
            .build();
    }

    private HttpConfiguration httpConfiguration() {
        var base = options == null ? HttpConfiguration.builder().build() : options;
        // The Power BI bearer token is applied explicitly by request().
        return base.toBuilder().auth(null).build();
    }
}