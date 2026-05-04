package io.kestra.plugin.microsoft.fabric.onelake;

import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.microsoft.fabric.AbstractFabricConnection;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.BufferedInputStream;
import java.net.URI;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: fabric_onelake_download
                namespace: company.team

                tasks:
                  - id: download_file
                    type: io.kestra.plugin.microsoft.fabric.onelake.Download
                    tenantId: "{{ secret('FABRIC_TENANT_ID') }}"
                    clientId: "{{ secret('FABRIC_CLIENT_ID') }}"
                    clientSecret: "{{ secret('FABRIC_CLIENT_SECRET') }}"
                    workspaceId: "your-workspace-id"
                    itemId: "your-lakehouse-item-id"
                    filePath: "data/myfile.csv"
                """
        )
    },
    metrics = {
        @Metric(name = "file.size", type = Counter.TYPE, description = "Size of the downloaded file in bytes")
    }
)
@Schema(
    title = "Download a file from Microsoft Fabric OneLake",
    description = """
        Downloads a file from OneLake using the ADLS Gen2 API and stores it in Kestra internal storage.
        The file is fetched from `Files/{filePath}` within the specified Lakehouse item.
        """
)
public class Download extends AbstractFabricConnection implements RunnableTask<Download.Output> {

    private static final String ONELAKE_ENDPOINT = "https://onelake.dfs.fabric.microsoft.com";

    @Schema(title = "Workspace ID", description = "Microsoft Fabric workspace GUID")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> workspaceId;

    @Schema(title = "Item ID", description = "Lakehouse or other Fabric item GUID within the workspace")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> itemId;

    @Schema(title = "File path", description = "Path within the item's Files directory to download (leading slash is stripped)")
    @NotNull
    @PluginProperty(group = "source")
    private Property<String> filePath;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rWorkspaceId = runContext.render(workspaceId).as(String.class).orElseThrow();
        var rItemId = runContext.render(itemId).as(String.class).orElseThrow();
        var rFilePath = stripLeadingSlash(runContext.render(filePath).as(String.class).orElseThrow());

        var credential = credentials(runContext);
        var serviceClient = new DataLakeServiceClientBuilder()
            .endpoint(ONELAKE_ENDPOINT)
            .credential(credential)
            .buildClient();

        var fsClient = serviceClient.getFileSystemClient(rWorkspaceId);
        var fileClient = fsClient.getFileClient(rItemId + "/Files/" + rFilePath);

        runContext.logger().info("Downloading from OneLake: {}/{}/Files/{}", rWorkspaceId, rItemId, rFilePath);

        var tempFile = runContext.workingDir().createTempFile().toFile();
        try (var os = new java.io.FileOutputStream(tempFile)) {
            fileClient.read(os);
        }

        var fileSize = tempFile.length();
        runContext.metric(Counter.of("file.size", fileSize));

        var storageUri = runContext.storage().putFile(tempFile);

        return Output.builder()
            .uri(storageUri.toString())
            .size(fileSize)
            .build();
    }

    private static String stripLeadingSlash(String path) {
        return path.startsWith("/") ? path.substring(1) : path;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Kestra storage URI", description = "Internal storage URI of the downloaded file")
        private final String uri;

        @Schema(title = "File size", description = "Size of the downloaded file in bytes")
        private final long size;
    }
}
