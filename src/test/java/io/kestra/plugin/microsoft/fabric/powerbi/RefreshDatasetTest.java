package io.kestra.plugin.microsoft.fabric.powerbi;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class RefreshDatasetTest {

    @Inject
    private RunContextFactory runContextFactory;

    private static WireMockServer wireMock;

    @BeforeAll
    static void startWireMock() {
        wireMock = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        wireMock.start();
        WireMock.configureFor(wireMock.port());
    }

    @AfterAll
    static void stopWireMock() {
        wireMock.stop();
    }

    @Test
    void happyPath_completedRefresh() throws Exception {
        var workspaceId = UUID.randomUUID().toString();
        var datasetId = UUID.randomUUID().toString();
        var refreshId = UUID.randomUUID().toString();
        var refreshesPath = "/v1/workspaces/" + workspaceId + "/semanticModels/" + datasetId + "/refreshes";
        var pollPath = refreshesPath + "/" + refreshId;

        wireMock.stubFor(post(urlEqualTo(refreshesPath))
            .willReturn(aResponse()
                .withStatus(202)
                .withHeader("Location", "http://localhost:" + wireMock.port() + pollPath)));

        wireMock.stubFor(get(urlEqualTo(pollPath))
            .willReturn(okJson("{\"refreshId\":\"" + refreshId + "\",\"status\":\"Completed\"}")));

        var runContext = runContextFactory.of();
        var task = TestableRefreshDataset.builder()
            .id(UUID.randomUUID().toString())
            .type(RefreshDataset.class.getName())
            .workspaceId(Property.ofValue(workspaceId))
            .datasetId(Property.ofValue(datasetId))
            .wait(Property.ofValue(true))
            .pollFrequency(Property.ofValue(Duration.ofMillis(100)))
            .timeout(Property.ofValue(Duration.ofSeconds(10)))
            .fabricApiBaseOverride("http://localhost:" + wireMock.port() + "/v1")
            .fakeToken("test-token")
            .build();

        var output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getRefreshId(), notNullValue());
        assertThat(output.getStatus(), is("Completed"));
    }

    @Test
    void noWait_returnsImmediately() throws Exception {
        var workspaceId = UUID.randomUUID().toString();
        var datasetId = UUID.randomUUID().toString();
        var refreshId = UUID.randomUUID().toString();
        var refreshesPath = "/v1/workspaces/" + workspaceId + "/semanticModels/" + datasetId + "/refreshes";
        var pollPath = refreshesPath + "/" + refreshId;

        wireMock.stubFor(post(urlEqualTo(refreshesPath))
            .willReturn(aResponse()
                .withStatus(202)
                .withHeader("Location", "http://localhost:" + wireMock.port() + pollPath)));

        var runContext = runContextFactory.of();
        var task = TestableRefreshDataset.builder()
            .id(UUID.randomUUID().toString())
            .type(RefreshDataset.class.getName())
            .workspaceId(Property.ofValue(workspaceId))
            .datasetId(Property.ofValue(datasetId))
            .wait(Property.ofValue(false))
            .fabricApiBaseOverride("http://localhost:" + wireMock.port() + "/v1")
            .fakeToken("test-token")
            .build();

        var output = task.run(runContext);

        assertThat(output.getStatus(), is("Running"));
        wireMock.verify(0, getRequestedFor(urlEqualTo(pollPath)));
    }
}
