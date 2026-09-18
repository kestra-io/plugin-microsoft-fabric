package io.kestra.plugin.microsoft.fabric.data.engineering;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class RunPipelineTest {

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
            .willReturn(okJson("{\"id\":\"" + runId + "\",\"status\":\"Completed\"}")));

        var runContext = runContextFactory.of();

        var task = TestableRunPipeline.builder()
            .id(UUID.randomUUID().toString())
            .type(RunPipeline.class.getName())
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
        assertThat(output.getStatus(), is("Completed"));
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
        var task = TestableRunPipeline.builder()
            .id(UUID.randomUUID().toString())
            .type(RunPipeline.class.getName())
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
        var task = TestableRunPipeline.builder()
            .id(UUID.randomUUID().toString())
            .type(RunPipeline.class.getName())
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

    @Test
    void kill_stopsPollingAndCancelsTheFabricJob() throws Exception {
        var workspaceId = UUID.randomUUID().toString();
        var pipelineId = UUID.randomUUID().toString();
        var runId = UUID.randomUUID().toString();
        var jobPath = "/v1/workspaces/" + workspaceId + "/items/" + pipelineId + "/jobs/instances";
        var pollPath = "/v1/jobs/instances/" + runId;

        wireMock.stubFor(post(urlPathEqualTo(jobPath))
            .willReturn(aResponse()
                .withStatus(202)
                .withHeader("Location", "http://localhost:" + wireMock.port() + pollPath)));

        // never terminal, so only a kill can end the poll loop
        wireMock.stubFor(get(urlEqualTo(pollPath))
            .willReturn(okJson("{\"id\":\"" + runId + "\",\"status\":\"Running\"}")));

        wireMock.stubFor(post(urlEqualTo(pollPath + "/cancel"))
            .willReturn(aResponse().withStatus(202)));

        var task = runningTask(workspaceId, pipelineId);
        var failure = runUntilPolling(task, pollPath);

        task.kill();

        assertThat(failure.done().await(10, TimeUnit.SECONDS), is(true));
        assertThat(failure.error().get(), instanceOf(InterruptedException.class));
        wireMock.verify(1, postRequestedFor(urlEqualTo(pollPath + "/cancel")));
    }

    @Test
    void stop_stopsPollingButLeavesTheFabricJobRunning() throws Exception {
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
            .willReturn(okJson("{\"id\":\"" + runId + "\",\"status\":\"Running\"}")));

        var task = runningTask(workspaceId, pipelineId);
        var failure = runUntilPolling(task, pollPath);

        task.stop();

        assertThat(failure.done().await(10, TimeUnit.SECONDS), is(true));
        assertThat(failure.error().get(), instanceOf(InterruptedException.class));
        wireMock.verify(0, postRequestedFor(urlEqualTo(pollPath + "/cancel")));
    }

    private TestableRunPipeline runningTask(String workspaceId, String pipelineId) {
        return TestableRunPipeline.builder()
            .id(UUID.randomUUID().toString())
            .type(RunPipeline.class.getName())
            .workspaceId(Property.ofValue(workspaceId))
            .pipelineId(Property.ofValue(pipelineId))
            .wait(Property.ofValue(true))
            .pollFrequency(Property.ofValue(Duration.ofMillis(100)))
            .timeout(Property.ofValue(Duration.ofSeconds(30)))
            .fabricApiBaseOverride("http://localhost:" + wireMock.port() + "/v1")
            .fakeToken("test-token")
            .build();
    }

    /** Runs the task on its own thread and returns once it has polled at least once, so a kill/stop lands mid-poll. */
    private TaskFailure runUntilPolling(TestableRunPipeline task, String pollPath) throws Exception {
        var runContext = runContextFactory.of();
        var done = new CountDownLatch(1);
        var error = new AtomicReference<Throwable>();

        var thread = new Thread(() -> {
            try {
                task.run(runContext);
            } catch (Throwable t) {
                error.set(t);
            } finally {
                done.countDown();
            }
        });
        thread.setDaemon(true);
        thread.start();

        var deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (wireMock.findAll(getRequestedFor(urlEqualTo(pollPath))).isEmpty()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("task never polled the job instance");
            }
            Thread.sleep(25);
        }

        return new TaskFailure(done, error);
    }

    private record TaskFailure(CountDownLatch done, AtomicReference<Throwable> error) {}
}
