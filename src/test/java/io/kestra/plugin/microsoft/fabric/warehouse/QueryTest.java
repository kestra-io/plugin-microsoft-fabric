package io.kestra.plugin.microsoft.fabric.warehouse;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class QueryTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void taskBuilds_withRequiredProperties() {
        var task = Query.builder()
            .id(UUID.randomUUID().toString())
            .type(Query.class.getName())
            .workspaceId(Property.ofValue("workspace-id"))
            .warehouseId(Property.ofValue("warehouse-id"))
            .sql(Property.ofValue("SELECT 1"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        assertThat(task, notNullValue());
        assertThat(task.getSql(), notNullValue());
        assertThat(task.getWarehouseId(), notNullValue());
    }

    @Test
    void liveQuery_storeResults() throws Exception {
        // Only runs when Fabric credentials are configured
        Assumptions.assumeTrue(System.getenv("FABRIC_TENANT_ID") != null,
            "Skipping live integration test: FABRIC_TENANT_ID not set");

        var runContext = runContextFactory.of();
        var task = Query.builder()
            .id(UUID.randomUUID().toString())
            .type(Query.class.getName())
            .tenantId(Property.ofValue(System.getenv("FABRIC_TENANT_ID")))
            .clientId(Property.ofValue(System.getenv("FABRIC_CLIENT_ID")))
            .clientSecret(Property.ofValue(System.getenv("FABRIC_CLIENT_SECRET")))
            .workspaceId(Property.ofValue(System.getenv("FABRIC_WORKSPACE_ID")))
            .warehouseId(Property.ofValue(System.getenv("FABRIC_WAREHOUSE_ID")))
            .sql(Property.ofValue("SELECT 1 AS one"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        var output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getRows(), hasSize(1));
        assertThat(output.getSize(), is(1L));
    }
}
