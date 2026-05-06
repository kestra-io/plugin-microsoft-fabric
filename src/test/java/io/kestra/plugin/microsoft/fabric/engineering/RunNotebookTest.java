package io.kestra.plugin.microsoft.fabric.engineering;

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
class RunNotebookTest {

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
    void happyPath_succeededNotebook() throws Exception {
        var workspaceId = UUID.randomUUID().toString();
        var notebookId = UUID.randomUUID().toString();
        var jobInstanceId = UUID.randomUUID().toString();
        var jobPath = "/v1/workspaces/" + workspaceId + "/items/" + notebookId + "/jobs/instances";
        var pollPath = "/v1/jobs/instances/" + jobInstanceId;

        wireMock.stubFor(post(urlPathEqualTo(jobPath))
            .willReturn(aResponse()
                .withStatus(202)
                .withHeader("Location", "http://localhost:" + wireMock.port() + pollPath)));

        wireMock.stubFor(get(urlEqualTo(pollPath))
            .willReturn(okJson("{\"id\":\"" + jobInstanceId + "\",\"status\":\"Completed\"}")));

        var runContext = runContextFactory.of();
        var task = TestableRunNotebook.builder()
            .id(UUID.randomUUID().toString())
            .type(RunNotebook.class.getName())
            .workspaceId(Property.ofValue(workspaceId))
            .notebookId(Property.ofValue(notebookId))
            .wait(Property.ofValue(true))
            .pollFrequency(Property.ofValue(Duration.ofMillis(100)))
            .timeout(Property.ofValue(Duration.ofSeconds(10)))
            .fabricApiBaseOverride("http://localhost:" + wireMock.port() + "/v1")
            .fakeToken("test-token")
            .build();

        var output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getJobInstanceId(), notNullValue());
        assertThat(output.getStatus(), is("Completed"));
    }
}
