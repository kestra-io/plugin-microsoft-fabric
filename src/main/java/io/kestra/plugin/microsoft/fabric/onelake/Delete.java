package io.kestra.plugin.microsoft.fabric.onelake;

import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.microsoft.fabric.AbstractFabricConnection;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

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
                id: fabric_onelake_delete
                namespace: company.team

                tasks:
                  - id: delete_file
                    type: io.kestra.plugin.microsoft.fabric.onelake.Delete
                    tenantId: "{{ secret('FABRIC_TENANT_ID') }}"
                    clientId: "{{ secret('FABRIC_CLIENT_ID') }}"
                    clientSecret: "{{ secret('FABRIC_CLIENT_SECRET') }}"
                    workspaceId: "your-workspace-id"
                    itemId: "your-lakehouse-item-id"
                    filePath: "data/myfile.csv"
                """
        )
    }
)
@Schema(
    title = "Delete a file from Microsoft Fabric OneLake",
    description = "Deletes a file at `Files/{filePath}` within the specified Lakehouse item in OneLake using the ADLS Gen2 API."
)
public class Delete extends AbstractFabricConnection implements RunnableTask<VoidOutput> {

    private static final String ONELAKE_ENDPOINT = "https://onelake.dfs.fabric.microsoft.com";

    @Schema(title = "Workspace ID", description = "Microsoft Fabric workspace GUID")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> workspaceId;

    @Schema(title = "Item ID", description = "Lakehouse or other Fabric item GUID within the workspace")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> itemId;

    @Schema(title = "File path", description = "Path within the item's Files directory to delete (leading slash is stripped)")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> filePath;

    @Schema(title = "Recursive", description = "Delete the path recursively if it is a directory")
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> recursive = Property.ofValue(Boolean.FALSE);

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var rWorkspaceId = runContext.render(workspaceId).as(String.class).orElseThrow();
        var rItemId = runContext.render(itemId).as(String.class).orElseThrow();
        var rFilePath = stripLeadingSlash(runContext.render(filePath).as(String.class).orElseThrow());
        var rRecursive = runContext.render(recursive).as(Boolean.class).orElse(Boolean.FALSE);

        var credential = credentials(runContext);
        var serviceClient = new DataLakeServiceClientBuilder()
            .endpoint(ONELAKE_ENDPOINT)
            .credential(credential)
            .buildClient();

        var fsClient = serviceClient.getFileSystemClient(rWorkspaceId);
        var fullPath = rItemId + "/Files/" + rFilePath;

        runContext.logger().info("Deleting from OneLake: {}/{}", rWorkspaceId, fullPath);

        if (Boolean.TRUE.equals(rRecursive)) {
            var dirClient = fsClient.getDirectoryClient(fullPath);
            dirClient.deleteRecursively();
        } else {
            var fileClient = fsClient.getFileClient(fullPath);
            fileClient.delete();
        }

        return null;
    }

    private static String stripLeadingSlash(String path) {
        return path.startsWith("/") ? path.substring(1) : path;
    }
}
