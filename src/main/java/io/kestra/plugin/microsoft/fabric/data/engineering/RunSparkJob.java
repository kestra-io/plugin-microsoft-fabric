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
                id: fabric_run_spark_job
                namespace: company.team

                tasks:
                  - id: run_spark_job
                    type: io.kestra.plugin.microsoft.fabric.data.engineering.RunSparkJob
                    tenantId: "{{ secret('FABRIC_TENANT_ID') }}"
                    clientId: "{{ secret('FABRIC_CLIENT_ID') }}"
                    clientSecret: "{{ secret('FABRIC_CLIENT_SECRET') }}"
                    workspaceId: "your-workspace-id"
                    sparkJobDefinitionId: "your-spark-job-definition-id"
                """
        )
    }
)
@Schema(
    title = "Run a Microsoft Fabric Spark Job Definition",
    description = """
        Submits a Fabric Spark Job Definition using the item job scheduler API and optionally waits for completion.
        Defaults: wait=true, pollFrequency=PT5S, timeout=PT1H.
        """
)
public class RunSparkJob extends AbstractEngineering implements RunnableTask<RunSparkJob.Output> {

    @Schema(title = "Spark Job Definition ID", description = "Microsoft Fabric Spark Job Definition item GUID within the workspace")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> sparkJobDefinitionId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rSparkJobDefinitionId = runContext.render(sparkJobDefinitionId).as(String.class).orElseThrow();
        var result = submitAndWait(runContext, rSparkJobDefinitionId, "sparkjob");
        return Output.builder()
            .jobInstanceId(result.jobInstanceId())
            .status(result.status())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Job instance ID", description = "Unique ID of the Spark job instance")
        private final String jobInstanceId;

        @Schema(title = "Final status", description = "Terminal status of the Spark job")
        private final String status;
    }
}
