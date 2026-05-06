package io.kestra.plugin.microsoft.fabric.onelake;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class DownloadTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void taskBuilds_withRequiredProperties() {
        var task = Download.builder()
            .id(UUID.randomUUID().toString())
            .type(Download.class.getName())
            .workspaceId(Property.ofValue("workspace-id"))
            .itemId(Property.ofValue("item-id"))
            .filePath(Property.ofValue("data/test.csv"))
            .build();

        assertThat(task, notNullValue());
        assertThat(task.getFilePath(), notNullValue());
        assertThat(task.getWorkspaceId(), notNullValue());
    }

    @Test
    void liveDownload_downloadsFromOneLake() throws Exception {
        Assumptions.assumeTrue(System.getenv("FABRIC_TENANT_ID") != null,
            "Skipping live integration test: FABRIC_TENANT_ID not set");

        var runContext = runContextFactory.of();
        var task = Download.builder()
            .id(UUID.randomUUID().toString())
            .type(Download.class.getName())
            .tenantId(Property.ofValue(System.getenv("FABRIC_TENANT_ID")))
            .clientId(Property.ofValue(System.getenv("FABRIC_CLIENT_ID")))
            .clientSecret(Property.ofValue(System.getenv("FABRIC_CLIENT_SECRET")))
            .workspaceId(Property.ofValue(System.getenv("FABRIC_WORKSPACE_ID")))
            .itemId(Property.ofValue(System.getenv("FABRIC_LAKEHOUSE_ID")))
            .filePath(Property.ofValue(System.getenv("FABRIC_TEST_FILE_PATH")))
            .build();

        var output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getUri(), notNullValue());
        assertThat(output.getSize(), greaterThan(0L));
    }
}
