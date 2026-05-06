package io.kestra.plugin.microsoft.fabric.onelake;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class UploadTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void taskBuilds_withRequiredProperties() {
        var task = Upload.builder()
            .id(UUID.randomUUID().toString())
            .type(Upload.class.getName())
            .workspaceId(Property.ofValue("workspace-id"))
            .itemId(Property.ofValue("item-id"))
            .filePath(Property.ofValue("data/test.csv"))
            .from(Property.ofValue("kestra:///test.csv"))
            .build();

        assertThat(task, notNullValue());
        assertThat(task.getFilePath(), notNullValue());
        assertThat(task.getFrom(), notNullValue());
    }

    @Test
    void liveUpload_uploadsToOneLake() throws Exception {
        Assumptions.assumeTrue(System.getenv("FABRIC_TENANT_ID") != null,
            "Skipping live integration test: FABRIC_TENANT_ID not set");

        var runContext = runContextFactory.of();

        // Create a small temp file and put it in Kestra storage
        var tempFile = File.createTempFile("test-upload", ".csv");
        try (var writer = new FileWriter(tempFile)) {
            writer.write("col1,col2\nval1,val2\n");
        }
        var storageUri = runContext.storage().putFile(tempFile);

        var task = Upload.builder()
            .id(UUID.randomUUID().toString())
            .type(Upload.class.getName())
            .tenantId(Property.ofValue(System.getenv("FABRIC_TENANT_ID")))
            .clientId(Property.ofValue(System.getenv("FABRIC_CLIENT_ID")))
            .clientSecret(Property.ofValue(System.getenv("FABRIC_CLIENT_SECRET")))
            .workspaceId(Property.ofValue(System.getenv("FABRIC_WORKSPACE_ID")))
            .itemId(Property.ofValue(System.getenv("FABRIC_LAKEHOUSE_ID")))
            .filePath(Property.ofValue("kestra-test/" + UUID.randomUUID() + ".csv"))
            .from(Property.ofValue(storageUri.toString()))
            .build();

        var output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getUri(), notNullValue());
        assertThat(output.getUri(), containsString("onelake.dfs.fabric.microsoft.com"));
    }
}
