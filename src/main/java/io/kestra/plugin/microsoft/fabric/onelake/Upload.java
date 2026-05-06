package io.kestra.plugin.microsoft.fabric.onelake;

import com.azure.core.util.BinaryData;
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

import java.io.InputStream;
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
                id: fabric_onelake_upload
                namespace: company.team

                tasks:
                  - id: upload_file
                    type: io.kestra.plugin.microsoft.fabric.onelake.Upload
                    tenantId: "{{ secret('FABRIC_TENANT_ID') }}"
                    clientId: "{{ secret('FABRIC_CLIENT_ID') }}"
                    clientSecret: "{{ secret('FABRIC_CLIENT_SECRET') }}"
                    workspaceId: "your-workspace-id"
                    itemId: "your-lakehouse-item-id"
                    filePath: "data/myfile.csv"
                    from: "{{ outputs.previous_task.uri }}"
                """
        )
    },
    metrics = {
        @Metric(name = "file.size", type = Counter.TYPE, description = "Size of the uploaded file in bytes")
    }
)
@Schema(
    title = "Upload a file to Microsoft Fabric OneLake",
    description = """
        Uploads a file from Kestra internal storage to OneLake using the ADLS Gen2 API.
        The file is placed at `Files/{filePath}` within the specified Lakehouse item.
        """
)
public class Upload extends AbstractFabricConnection implements RunnableTask<Upload.Output> {

    private static final String ONELAKE_ENDPOINT = "https://onelake.dfs.fabric.microsoft.com";

    @Schema(title = "Workspace ID", description = "Microsoft Fabric workspace GUID")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> workspaceId;

    @Schema(title = "Item ID", description = "Lakehouse or other Fabric item GUID within the workspace")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> itemId;

    @Schema(title = "File path", description = "Destination path within the item's Files directory (leading slash is stripped)")
    @NotNull
    @PluginProperty(group = "destination")
    private Property<String> filePath;

    @Schema(title = "Source URI", description = "Kestra internal storage URI of the file to upload")
    @NotNull
    @PluginProperty(internalStorageURI = true, group = "source")
    private Property<String> from;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rWorkspaceId = runContext.render(workspaceId).as(String.class).orElseThrow();
        var rItemId = runContext.render(itemId).as(String.class).orElseThrow();
        var rFilePath = stripLeadingSlash(runContext.render(filePath).as(String.class).orElseThrow());
        var rFrom = runContext.render(from).as(String.class).orElseThrow();

        var credential = credentials(runContext);
        var serviceClient = new DataLakeServiceClientBuilder()
            .endpoint(ONELAKE_ENDPOINT)
            .credential(credential)
            .buildClient();

        var fsClient = serviceClient.getFileSystemClient(rWorkspaceId);
        var fileClient = fsClient.getFileClient(rItemId + "/Files/" + rFilePath);

        runContext.logger().info("Uploading to OneLake: {}/{}/Files/{}", rWorkspaceId, rItemId, rFilePath);

        try (InputStream is = runContext.storage().getFile(URI.create(rFrom))) {
            var data = BinaryData.fromStream(is);
            fileClient.upload(data, true);
        }

        var fileSize = fileClient.getProperties().getFileSize();
        runContext.metric(Counter.of("file.size", fileSize));

        return Output.builder()
            .uri("abfss://" + rWorkspaceId + "@onelake.dfs.fabric.microsoft.com/" + rItemId + "/Files/" + rFilePath)
            .build();
    }

    private static String stripLeadingSlash(String path) {
        return path.startsWith("/") ? path.substring(1) : path;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "OneLake URI", description = "ABFSS URI of the uploaded file in OneLake")
        private final String uri;
    }
}
