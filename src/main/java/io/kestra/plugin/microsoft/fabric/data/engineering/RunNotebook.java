package io.kestra.plugin.microsoft.fabric.data.engineering;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
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
                id: fabric_run_notebook
                namespace: company.team

                tasks:
                  - id: run_notebook
                    type: io.kestra.plugin.microsoft.fabric.data.engineering.RunNotebook
                    tenantId: "{{ secret('FABRIC_TENANT_ID') }}"
                    clientId: "{{ secret('FABRIC_CLIENT_ID') }}"
                    clientSecret: "{{ secret('FABRIC_CLIENT_SECRET') }}"
                    workspaceId: "your-workspace-id"
                    notebookId: "your-notebook-item-id"
                    parameters:
                      input_path: "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Files/input.csv"
                """
        )
    }
)
@Schema(
    title = "Run a Microsoft Fabric Notebook",
    description = """
        Submits a Fabric Notebook job using the item job scheduler API and optionally waits for completion.
        Defaults: wait=true, pollFrequency=PT5S, timeout=PT1H.
        """
)
public class RunNotebook extends AbstractEngineering implements RunnableTask<RunNotebook.Output> {

    @Schema(title = "Notebook ID", description = "Microsoft Fabric Notebook item GUID within the workspace")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> notebookId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rNotebookId = runContext.render(notebookId).as(String.class).orElseThrow();
        var result = submitAndWait(runContext, rNotebookId, "RunNotebook");
        return Output.builder()
            .jobInstanceId(result.jobInstanceId())
            .status(result.status())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Job instance ID", description = "Unique ID of the notebook job instance")
        private final String jobInstanceId;

        @Schema(title = "Final status", description = "Terminal status of the notebook job")
        private final String status;
    }
}
