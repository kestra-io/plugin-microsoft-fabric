package io.kestra.plugin.microsoft.fabric.powerbi;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.micronaut.test.annotation.MockBean;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@WireMockTest
class RefreshGroupDatasetTest {
    @Inject
    private RunContextFactory runContextFactory;

    @MockBean(StorageInterface.class)
    StorageInterface storageInterface() {
        return Mockito.mock(StorageInterface.class);
    }

    @Test
    void refreshWaitsAndReturnsLegacyOutputAndPowerBiAuthentication(WireMockRuntimeInfo wm) throws Exception {
        stubAuth(wm);
        var requestId = UUID.randomUUID().toString();
        var path = refreshesPath();
        stubFor(post(urlEqualTo(path))
            .willReturn(aResponse().withStatus(202).withHeader("requestId", requestId).withBody("{}")));
        stubFor(get(urlEqualTo(path))
            .willReturn(okJson("""
                {"value":[{"requestId":"%s","refreshType":"Full","startTime":"2026-10-14T10:00:00Z","endTime":"2026-10-14T10:01:00Z","status":"Completed","extendedStatus":"Completed"}]}
                """.formatted(requestId))));

        var result = task(wm).wait(Property.ofValue(true)).pollDuration(Property.ofValue(Duration.ofMillis(5)))
            .waitDuration(Property.ofValue(Duration.ofSeconds(1))).build().run(runContextFactory.of());

        assertThat(result.getRequestId(), is(requestId));
        assertThat(result.getStatus(), is("Completed"));
        assertThat(result.getRefreshType(), is("Full"));
        assertThat(result.getExtendedStatus(), is("Completed"));
        assertThat(result.getStartTime().toString(), is("2026-10-14T10:00:00Z"));
        verify(postRequestedFor(urlEqualTo("/login/tenant/oauth2/token"))
            .withHeader("Content-Type", containing("application/x-www-form-urlencoded"))
            .withRequestBody(containing("grant_type=client_credentials"))
            .withRequestBody(containing("resource=https%3A%2F%2Fanalysis.windows.net%2Fpowerbi%2Fapi"))
            .withRequestBody(containing("scope=https%3A%2F%2Fanalysis.windows.net%2Fpowerbi%2Fapi")));
        verify(postRequestedFor(urlEqualTo(path)).withHeader("Authorization", equalTo("Bearer test-token")));
    }

    @Test
    void noWaitReturnsRequestIdWithoutPolling(WireMockRuntimeInfo wm) throws Exception {
        stubAuth(wm);
        var requestId = UUID.randomUUID().toString();
        stubFor(post(urlEqualTo(refreshesPath()))
            .willReturn(aResponse().withStatus(202).withHeader("requestId", requestId)));

        var output = task(wm).build().run(runContextFactory.of());

        assertThat(output.getRequestId(), is(requestId));
        assertThat(output.getStatus(), nullValue());
        verify(0, getRequestedFor(urlEqualTo(refreshesPath())));
        var postRequests = findAll(postRequestedFor(urlEqualTo(refreshesPath())));
        assertThat(postRequests, hasSize(1));
        assertThat(postRequests.get(0).getBodyAsString(), is(""));
    }

    @Test
    void noWaitAcceptsDocumentedMsRequestIdHeader(WireMockRuntimeInfo wm) throws Exception {
        stubAuth(wm);
        var requestId = UUID.randomUUID().toString();
        stubFor(post(urlEqualTo(refreshesPath()))
            .willReturn(aResponse().withStatus(202).withHeader("x-ms-request-id", requestId)));

        var output = task(wm).build().run(runContextFactory.of());

        assertThat(output.getRequestId(), is(requestId));
        assertThat(output.getStatus(), nullValue());
        verify(0, getRequestedFor(urlEqualTo(refreshesPath())));
    }

    @Test
    void noWaitCanExtractRequestIdFromLocationHeader(WireMockRuntimeInfo wm) throws Exception {
        stubAuth(wm);
        var requestId = UUID.randomUUID().toString();
        var location = "https://api.powerbi.com/v1.0/myorg/groups/group/datasets/dataset/refreshes/" + requestId;
        stubFor(post(urlEqualTo(refreshesPath()))
            .willReturn(aResponse().withStatus(202).withHeader("Location", location)));

        var output = task(wm).build().run(runContextFactory.of());

        assertThat(output.getRequestId(), is(requestId));
        assertThat(output.getStatus(), nullValue());
        verify(0, getRequestedFor(urlEqualTo(refreshesPath())));
    }

    @Test
    void dynamicIdentifiersAreEncodedAsSinglePathSegments(WireMockRuntimeInfo wm) throws Exception {
        var tenantId = "tenant/../?tenant=1";
        var groupId = "group/../?x=y";
        var datasetId = "dataset/../?d=1";
        var tokenPath = "/login/tenant%2F%2E%2E%2F%3Ftenant%3D1/oauth2/token";
        var refreshPath = "/api/v1.0/myorg/groups/group%2F%2E%2E%2F%3Fx%3Dy/datasets/dataset%2F%2E%2E%2F%3Fd%3D1/refreshes";
        var requestId = UUID.randomUUID().toString();

        stubFor(post(urlEqualTo(tokenPath)).willReturn(okJson("{\"access_token\":\"test-token\"}")));
        stubFor(post(urlEqualTo(refreshPath))
            .willReturn(aResponse().withStatus(202).withHeader("requestId", requestId)));

        var output = task(wm)
            .tenantId(Property.ofValue(tenantId))
            .groupId(Property.ofValue(groupId))
            .datasetId(Property.ofValue(datasetId))
            .build()
            .run(runContextFactory.of());

        assertThat(output.getRequestId(), is(requestId));
        verify(postRequestedFor(urlEqualTo(tokenPath)));
        verify(postRequestedFor(urlEqualTo(refreshPath)));
    }

    @Test
    void missingRequestIdHeaderFailsClearly(WireMockRuntimeInfo wm) throws Exception {
        stubAuth(wm);
        stubFor(post(urlEqualTo(refreshesPath())).willReturn(aResponse().withStatus(202).withBody("{}")));

        var error = assertThrows(IllegalStateException.class, () -> task(wm).build().run(runContextFactory.of()));
        assertThat(error.getMessage(), containsString("missing requestId header"));
    }

    @Test
    void blankRequestIdHeaderIsRejected(WireMockRuntimeInfo wm) throws Exception {
        stubAuth(wm);
        stubFor(post(urlEqualTo(refreshesPath())).willReturn(aResponse().withStatus(202).withHeader("requestId", " ")));

        var error = assertThrows(IllegalStateException.class, () -> task(wm).build().run(runContextFactory.of()));
        assertThat(error.getMessage(), containsString("missing requestId header"));
    }

    @Test
    void unexpectedPollingHttpStatusFailsClearly(WireMockRuntimeInfo wm) throws Exception {
        stubAuth(wm);
        var requestId = UUID.randomUUID().toString();
        stubFor(post(urlEqualTo(refreshesPath())).willReturn(aResponse().withStatus(202).withHeader("requestId", requestId)));
        stubFor(get(urlEqualTo(refreshesPath())).willReturn(aResponse().withStatus(500).withBody("service error")));

        var error = assertThrows(IllegalStateException.class, () -> task(wm)
            .wait(Property.ofValue(true)).pollDuration(Property.ofValue(Duration.ofMillis(5)))
            .waitDuration(Property.ofValue(Duration.ofSeconds(1))).build().run(runContextFactory.of()));
        assertThat(error.getMessage(), containsString("polling failed with HTTP 500"));
    }

    @Test
    void failedRefreshThrowsInsteadOfReturningSuccess(WireMockRuntimeInfo wm) throws Exception {
        stubAuth(wm);
        var requestId = UUID.randomUUID().toString();
        stubFor(post(urlEqualTo(refreshesPath())).willReturn(aResponse().withStatus(202).withHeader("requestId", requestId)));
        stubFor(get(urlEqualTo(refreshesPath())).willReturn(okJson("""
            {"value":[{"requestId":"%s","status":"Failed","extendedStatus":"ModelRefreshFailed"}]}
            """.formatted(requestId))));

        var error = assertThrows(Exception.class, () -> task(wm)
            .wait(Property.ofValue(true)).pollDuration(Property.ofValue(Duration.ofMillis(5)))
            .waitDuration(Property.ofValue(Duration.ofSeconds(1))).build().run(runContextFactory.of()));
        assertThat(error.getMessage(), containsString("Power BI refresh failed with status 'Failed'"));
    }

    @Test
    void timeoutIsBoundedByWaitDuration(WireMockRuntimeInfo wm) throws Exception {
        stubAuth(wm);
        var requestId = UUID.randomUUID().toString();
        stubFor(post(urlEqualTo(refreshesPath())).willReturn(aResponse().withStatus(202).withHeader("requestId", requestId)));
        stubFor(get(urlEqualTo(refreshesPath())).willReturn(okJson("""
            {"value":[{"requestId":"%s","status":"Unknown"}]}
            """.formatted(requestId))));

        assertThrows(TimeoutException.class, () -> task(wm)
            .wait(Property.ofValue(true)).pollDuration(Property.ofValue(Duration.ofMillis(5)))
            .waitDuration(Property.ofValue(Duration.ofMillis(100))).build().run(runContextFactory.of()));
    }

    @Test
    void pollingRetriesAfterHttp429(WireMockRuntimeInfo wm) throws Exception {
        stubAuth(wm);
        var requestId = UUID.randomUUID().toString();
        stubFor(post(urlEqualTo(refreshesPath())).willReturn(aResponse().withStatus(202).withHeader("requestId", requestId)));
        stubFor(get(urlEqualTo(refreshesPath()))
            .inScenario("powerbi-throttling")
            .whenScenarioStateIs(STARTED)
            .willReturn(aResponse().withStatus(429).withHeader("Retry-After", "0"))
            .willSetStateTo("after-throttle"));
        stubFor(get(urlEqualTo(refreshesPath()))
            .inScenario("powerbi-throttling")
            .whenScenarioStateIs("after-throttle")
            .willReturn(okJson("""
                {"value":[{"requestId":"%s","status":"Completed"}]}
                """.formatted(requestId))));

        var output = task(wm).wait(Property.ofValue(true)).pollDuration(Property.ofValue(Duration.ofMillis(5)))
            .waitDuration(Property.ofValue(Duration.ofSeconds(1))).build().run(runContextFactory.of());

        assertThat(output.getStatus(), is("Completed"));
        verify(2, getRequestedFor(urlEqualTo(refreshesPath())));
    }

    @Test
    void explicitlyConfiguredNotificationIsSent(WireMockRuntimeInfo wm) throws Exception {
        stubAuth(wm);
        stubFor(post(urlEqualTo(refreshesPath()))
            .withRequestBody(matchingJsonPath("$.notifyOption", equalTo("MailOnFailure")))
            .willReturn(aResponse().withStatus(202).withHeader("requestId", UUID.randomUUID().toString())));

        task(wm).notifyOption(Property.ofValue(RefreshGroupDataset.NotifyOption.MailOnFailure))
            .build().run(runContextFactory.of());

        verify(postRequestedFor(urlEqualTo(refreshesPath()))
            .withRequestBody(matchingJsonPath("$.notifyOption", equalTo("MailOnFailure"))));
    }

    @Test
    void legacyTaskTypeIsRegisteredAsAnAlias() {
        assertThat(
            io.kestra.core.models.Plugin.getAliases(RefreshGroupDataset.class),
            hasItem("io.kestra.plugin.powerbi.RefreshGroupDataset")
        );
    }

    private static void stubAuth(WireMockRuntimeInfo wm) {
        stubFor(post(urlEqualTo("/login/tenant/oauth2/token"))
            .willReturn(okJson("{\"access_token\":\"test-token\"}")));
    }

    private static String refreshesPath() {
        return "/api/v1.0/myorg/groups/group/datasets/dataset/refreshes";
    }

    private static TestableRefreshGroupDataset.TestableRefreshGroupDatasetBuilder<?, ?> task(WireMockRuntimeInfo wm) {
        var base = "http://localhost:" + wm.getHttpPort();
        return TestableRefreshGroupDataset.builder()
            .id(UUID.randomUUID().toString())
            .type(RefreshGroupDataset.class.getName())
            .tenantId(Property.ofValue("tenant"))
            .clientId(Property.ofValue("client"))
            .clientSecret(Property.ofValue("secret"))
            .groupId(Property.ofValue("group"))
            .datasetId(Property.ofValue("dataset"))
            .loginUrlOverride(base + "/login")
            .apiUrlOverride(base + "/api");
    }
}