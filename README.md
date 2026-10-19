<p align="center">
  <a href="https://www.kestra.io">
    <img src="https://kestra.io/banner.png" alt="Kestra workflow orchestrator" />
  </a>
</p>

# Kestra Microsoft Fabric Plugin

This plugin adds Microsoft Fabric and Power BI tasks to [Kestra](https://kestra.io), an open-source orchestration platform. It supports OneLake file operations, Fabric Data Engineering jobs, Warehouse queries, and Power BI semantic-model refreshes.

## Available tasks

| Task | Description |
| --- | --- |
| `io.kestra.plugin.microsoft.fabric.data.engineering.RunPipeline` | Run a Fabric Data Pipeline. |
| `io.kestra.plugin.microsoft.fabric.data.engineering.RunNotebook` | Run a Fabric notebook. |
| `io.kestra.plugin.microsoft.fabric.data.engineering.RunSparkJob` | Run a Fabric Spark Job Definition. |
| `io.kestra.plugin.microsoft.fabric.onelake.Upload` | Upload a file to OneLake. |
| `io.kestra.plugin.microsoft.fabric.onelake.Download` | Download a file from OneLake. |
| `io.kestra.plugin.microsoft.fabric.onelake.Delete` | Delete a file from OneLake. |
| `io.kestra.plugin.microsoft.fabric.warehouse.Query` | Execute a SQL query against a Fabric Warehouse. |
| `io.kestra.plugin.microsoft.fabric.powerbi.RefreshGroupDataset` | Refresh a Power BI dataset and optionally wait for completion. |

### Power BI compatibility

The canonical task type is `io.kestra.plugin.microsoft.fabric.powerbi.RefreshGroupDataset`. Existing flows using the legacy type `io.kestra.plugin.powerbi.RefreshGroupDataset` remain supported through a task alias.

The Power BI refresh task uses Azure service-principal credentials and the Power BI API OAuth resource/scope. Its legacy property names, defaults, and outputs are retained. Advanced properties `loginUrl` and `apiUrl` default to the public Microsoft identity and Power BI API endpoints; `oauthResource` and `oauthScope` default to the corresponding public-cloud values. Configure all four to match an alternate cloud. `notifyOption` is optional and does not alter the request when omitted. During `wait: true`, a 401 polling response triggers one OAuth token refresh and a single retry for that poll.

## Example

```yaml
id: refresh-powerbi-dataset
namespace: company.team

tasks:
  - id: refresh_sales_model
    type: io.kestra.plugin.microsoft.fabric.powerbi.RefreshGroupDataset
    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
    groupId: "9f1c2b8c-12ab-4f3e-9bcd-0af2c5e6d123"
    datasetId: "5c8b4a1e-7f89-4c33-9dd8-1f23c4567890"
    wait: true
    pollDuration: PT5S
    waitDuration: PT10M
```

The service principal must have permission to refresh the target dataset. See the [Power BI dataset refresh API](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/refresh-dataset-in-group) and the [Kestra plugin developer guide](https://kestra.io/docs/plugin-developer-guide).
