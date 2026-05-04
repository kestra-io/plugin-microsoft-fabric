package io.kestra.plugin.microsoft.fabric.onelake;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class DeleteTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void taskBuilds_withRequiredProperties() {
        var task = Delete.builder()
            .id(UUID.randomUUID().toString())
            .type(Delete.class.getName())
            .workspaceId(Property.ofValue("workspace-id"))
            .itemId(Property.ofValue("item-id"))
            .filePath(Property.ofValue("data/test.csv"))
            .build();

        assertThat(task, notNullValue());
        assertThat(task.getFilePath(), notNullValue());
        assertThat(task.getWorkspaceId(), notNullValue());
    }

    @Test
    void liveDelete_deletesFromOneLake() throws Exception {
        Assumptions.assumeTrue(System.getenv("FABRIC_TENANT_ID") != null,
            "Skipping live integration test: FABRIC_TENANT_ID not set");

        var runContext = runContextFactory.of();

        // Upload a file first so we can delete it
        var tempFile = File.createTempFile("delete-test", ".csv");
        try (var writer = new FileWriter(tempFile)) {
            writer.write("col1\nval1\n");
        }
        var storageUri = runContext.storage().putFile(tempFile);
        var filePath = "kestra-delete-test/" + UUID.randomUUID() + ".csv";

        Upload.builder()
            .id(UUID.randomUUID().toString())
            .type(Upload.class.getName())
            .tenantId(Property.ofValue(System.getenv("FABRIC_TENANT_ID")))
            .clientId(Property.ofValue(System.getenv("FABRIC_CLIENT_ID")))
            .clientSecret(Property.ofValue(System.getenv("FABRIC_CLIENT_SECRET")))
            .workspaceId(Property.ofValue(System.getenv("FABRIC_WORKSPACE_ID")))
            .itemId(Property.ofValue(System.getenv("FABRIC_LAKEHOUSE_ID")))
            .filePath(Property.ofValue(filePath))
            .from(Property.ofValue(storageUri.toString()))
            .build()
            .run(runContext);

        var deleteTask = Delete.builder()
            .id(UUID.randomUUID().toString())
            .type(Delete.class.getName())
            .tenantId(Property.ofValue(System.getenv("FABRIC_TENANT_ID")))
            .clientId(Property.ofValue(System.getenv("FABRIC_CLIENT_ID")))
            .clientSecret(Property.ofValue(System.getenv("FABRIC_CLIENT_SECRET")))
            .workspaceId(Property.ofValue(System.getenv("FABRIC_WORKSPACE_ID")))
            .itemId(Property.ofValue(System.getenv("FABRIC_LAKEHOUSE_ID")))
            .filePath(Property.ofValue(filePath))
            .build();

        // Should complete without exception
        deleteTask.run(runContext);
    }
}
