package io.kestra.plugin.microsoft.fabric.pipeline;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
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
class RunTest {

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
    void happyPath_succeededPipeline() throws Exception {
        var workspaceId = UUID.randomUUID().toString();
        var pipelineId = UUID.randomUUID().toString();
        var runId = UUID.randomUUID().toString();
        var jobPath = "/v1/workspaces/" + workspaceId + "/items/" + pipelineId + "/jobs/instances";
        var pollPath = "/v1/jobs/instances/" + runId;

        wireMock.stubFor(post(urlPathEqualTo(jobPath))
            .willReturn(aResponse()
                .withStatus(202)
                .withHeader("Location", "http://localhost:" + wireMock.port() + pollPath)));

        wireMock.stubFor(get(urlEqualTo(pollPath))
            .willReturn(okJson("{\"id\":\"" + runId + "\",\"status\":\"Succeeded\"}")));

        var runContext = runContextFactory.of();

        var task = TestableRun.builder()
            .id(UUID.randomUUID().toString())
            .type(Run.class.getName())
            .workspaceId(Property.ofValue(workspaceId))
            .pipelineId(Property.ofValue(pipelineId))
            .wait(Property.ofValue(true))
            .pollFrequency(Property.ofValue(Duration.ofMillis(100)))
            .timeout(Property.ofValue(Duration.ofSeconds(10)))
            .fabricApiBaseOverride("http://localhost:" + wireMock.port() + "/v1")
            .fakeToken("test-token")
            .build();

        var output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getRunId(), notNullValue());
        assertThat(output.getStatus(), is("Succeeded"));
    }

    @Test
    void noWait_returnsImmediately() throws Exception {
        var workspaceId = UUID.randomUUID().toString();
        var pipelineId = UUID.randomUUID().toString();
        var runId = UUID.randomUUID().toString();
        var jobPath = "/v1/workspaces/" + workspaceId + "/items/" + pipelineId + "/jobs/instances";
        var pollPath = "/v1/jobs/instances/" + runId;

        wireMock.stubFor(post(urlPathEqualTo(jobPath))
            .willReturn(aResponse()
                .withStatus(202)
                .withHeader("Location", "http://localhost:" + wireMock.port() + pollPath)));

        var runContext = runContextFactory.of();
        var task = TestableRun.builder()
            .id(UUID.randomUUID().toString())
            .type(Run.class.getName())
            .workspaceId(Property.ofValue(workspaceId))
            .pipelineId(Property.ofValue(pipelineId))
            .wait(Property.ofValue(false))
            .fabricApiBaseOverride("http://localhost:" + wireMock.port() + "/v1")
            .fakeToken("test-token")
            .build();

        var output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getStatus(), is("Running"));
        wireMock.verify(0, getRequestedFor(urlEqualTo(pollPath)));
    }

    @Test
    void failedPipeline_throwsException() {
        var workspaceId = UUID.randomUUID().toString();
        var pipelineId = UUID.randomUUID().toString();
        var runId = UUID.randomUUID().toString();
        var jobPath = "/v1/workspaces/" + workspaceId + "/items/" + pipelineId + "/jobs/instances";
        var pollPath = "/v1/jobs/instances/" + runId;

        wireMock.stubFor(post(urlPathEqualTo(jobPath))
            .willReturn(aResponse()
                .withStatus(202)
                .withHeader("Location", "http://localhost:" + wireMock.port() + pollPath)));

        wireMock.stubFor(get(urlEqualTo(pollPath))
            .willReturn(okJson("{\"id\":\"" + runId + "\",\"status\":\"Failed\"}")));

        var runContext = runContextFactory.of();
        var task = TestableRun.builder()
            .id(UUID.randomUUID().toString())
            .type(Run.class.getName())
            .workspaceId(Property.ofValue(workspaceId))
            .pipelineId(Property.ofValue(pipelineId))
            .wait(Property.ofValue(true))
            .pollFrequency(Property.ofValue(Duration.ofMillis(100)))
            .timeout(Property.ofValue(Duration.ofSeconds(5)))
            .fabricApiBaseOverride("http://localhost:" + wireMock.port() + "/v1")
            .fakeToken("test-token")
            .build();

        org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class, () -> task.run(runContext));
    }
}
