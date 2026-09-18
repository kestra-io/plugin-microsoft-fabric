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
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: fabric_pipeline_run
                namespace: company.team

                tasks:
                  - id: run_pipeline
                    type: io.kestra.plugin.microsoft.fabric.data.engineering.RunPipeline
                    tenantId: "{{ secret('FABRIC_TENANT_ID') }}"
                    clientId: "{{ secret('FABRIC_CLIENT_ID') }}"
                    clientSecret: "{{ secret('FABRIC_CLIENT_SECRET') }}"
                    workspaceId: "your-workspace-id"
                    pipelineId: "your-pipeline-id"
                    parameters:
                      myParam: "myValue"
                """
        )
    }
)
@Schema(
    title = "Run a Microsoft Fabric Data Pipeline",
    description = """
        Triggers a Fabric Data Pipeline job and optionally waits for completion.
        Posts to the Fabric scheduler API with `jobType=Pipeline`, polls the returned job instance URL,
        and fails if the pipeline ends in `Failed`, `Cancelled`, or `Deduped` state.
        Defaults: wait=true, pollFrequency=PT5S, timeout=PT1H.
        """
)
public class RunPipeline extends AbstractEngineering implements RunnableTask<RunPipeline.Output> {

    @Schema(title = "Pipeline ID", description = "Microsoft Fabric Data Pipeline item GUID")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> pipelineId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rPipelineId = runContext.render(pipelineId).as(String.class).orElseThrow();
        var result = submitAndWait(runContext, rPipelineId, "Pipeline");
        return Output.builder()
            .runId(result.jobInstanceId())
            .status(result.status())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Job instance ID", description = "Unique ID of the pipeline job instance")
        private final String runId;

        @Schema(title = "Final status", description = "Terminal status of the pipeline job: Completed, Failed, Cancelled, or Deduped")
        private final String status;
    }
}
