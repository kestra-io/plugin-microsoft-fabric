package io.kestra.plugin.microsoft.fabric.warehouse;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.microsoft.fabric.AbstractFabricConnection;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
                id: fabric_warehouse_query
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.microsoft.fabric.warehouse.Query
                    tenantId: "{{ secret('FABRIC_TENANT_ID') }}"
                    clientId: "{{ secret('FABRIC_CLIENT_ID') }}"
                    clientSecret: "{{ secret('FABRIC_CLIENT_SECRET') }}"
                    sqlEndpointId: "your-sql-endpoint-id"
                    sql: "SELECT TOP 100 * FROM dbo.sales"
                    fetchType: STORE
                """
        )
    },
    metrics = {
        @Metric(name = "rows", type = Counter.TYPE, description = "Number of rows returned by the query")
    }
)
@Schema(
    title = "Query a Microsoft Fabric Warehouse",
    description = """
        Executes a SQL query against a Microsoft Fabric Warehouse over JDBC using Active Directory service principal authentication.
        Supports STORE (writes to Kestra internal storage as ION) and FETCH (returns rows as a list).
        """
)
public class Query extends AbstractFabricConnection implements RunnableTask<Query.Output> {

    @Schema(
        title = "SQL Endpoint ID",
        description = "Unique identifier of the Fabric Warehouse SQL endpoint, used as the JDBC server hostname " +
            "(<sqlEndpointId>.datawarehouse.fabric.microsoft.com). " +
            "Find it in the Fabric portal: open your Warehouse → Settings → SQL endpoint → copy the unique identifier from the SQL connection string."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> sqlEndpointId;

    @Schema(title = "SQL query", description = "SQL statement to execute against the warehouse")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> sql;

    @Schema(
        title = "Fetch type",
        description = "How to return results: STORE writes an ION file to Kestra storage; FETCH returns rows as a list (not recommended for large result sets)"
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rWarehouseId = runContext.render(sqlEndpointId).as(String.class).orElseThrow();
        var rSql = runContext.render(sql).as(String.class).orElseThrow();
        var rFetchType = runContext.render(fetchType).as(FetchType.class).orElse(FetchType.STORE);

        var token = warehouseToken(runContext);

        var ds = new SQLServerDataSource();
        ds.setServerName(rWarehouseId + ".datawarehouse.fabric.microsoft.com");
        ds.setPortNumber(1433);
        ds.setDatabaseName(rWarehouseId);
        ds.setEncrypt("true");
        ds.setTrustServerCertificate(false);
        ds.setLoginTimeout(30);
        ds.setResponseBuffering("adaptive");
        ds.setAccessToken(token);

        runContext.logger().info("Executing SQL query on warehouse '{}'", rWarehouseId);

        try (var connection = ds.getConnection();
             var stmt = connection.prepareStatement(rSql);
             var rs = stmt.executeQuery()) {

            return switch (rFetchType) {
                case STORE -> storeResults(runContext, rs);
                case FETCH -> fetchResults(runContext, rs);
                case FETCH_ONE -> fetchOneResult(runContext, rs);
                case NONE -> Output.builder().size(0L).build();
            };
        }
    }

    private Output storeResults(RunContext runContext, ResultSet rs) throws Exception {
        var tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        long count;
        try (var writer = new BufferedWriter(new FileWriter(tempFile))) {
            var rows = Flux.create(emitter -> {
                try {
                    var meta = rs.getMetaData();
                    while (rs.next()) {
                        emitter.next(rowToMap(rs, meta));
                    }
                    emitter.complete();
                } catch (Exception e) {
                    emitter.error(e);
                }
            });
            count = FileSerde.writeAll(writer, rows).blockOptional().orElse(0L);
        }
        runContext.metric(Counter.of("rows", count));
        var storageUri = runContext.storage().putFile(tempFile);
        return Output.builder().uri(storageUri).size(count).build();
    }

    private Output fetchResults(RunContext runContext, ResultSet rs) throws Exception {
        var meta = rs.getMetaData();
        var rows = new ArrayList<Map<String, Object>>();
        while (rs.next()) {
            rows.add(rowToMap(rs, meta));
        }
        runContext.metric(Counter.of("rows", rows.size()));
        return Output.builder().rows(rows).size((long) rows.size()).build();
    }

    private Output fetchOneResult(RunContext runContext, ResultSet rs) throws Exception {
        var meta = rs.getMetaData();
        Map<String, Object> row = rs.next() ? rowToMap(rs, meta) : null;
        runContext.metric(Counter.of("rows", row == null ? 0 : 1));
        return Output.builder().row(row).size(row == null ? 0L : 1L).build();
    }

    private static Map<String, Object> rowToMap(ResultSet rs, ResultSetMetaData meta) throws Exception {
        var map = new LinkedHashMap<String, Object>();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            map.put(meta.getColumnLabel(i), rs.getObject(i));
        }
        return map;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Storage URI", description = "Kestra internal storage URI of the ION file when fetchType=STORE")
        private final URI uri;

        @Schema(title = "Rows", description = "Result rows when fetchType=FETCH")
        private final List<Map<String, Object>> rows;

        @Schema(title = "Row", description = "Single result row when fetchType=FETCH_ONE")
        private final Map<String, Object> row;

        @Schema(title = "Row count", description = "Number of rows returned")
        private final long size;
    }
}
