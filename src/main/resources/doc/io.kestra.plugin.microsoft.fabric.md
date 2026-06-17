# How to use the Microsoft Fabric plugin

Interact with Microsoft Fabric — OneLake storage, Warehouse SQL, notebooks, Spark jobs, and pipelines — from Kestra flows.

## Authentication

Set `tenantId`, `clientId`, and `clientSecret` for service principal authentication. If all three are provided, the plugin uses `ClientSecretCredential`; otherwise it falls back to `DefaultAzureCredential`. Store secrets in [secrets](https://kestra.io/docs/concepts/secret) and apply connection properties globally with [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults).

## Tasks

### OneLake

`onelake.Download` downloads a file from OneLake — set `workspaceId`, `itemId`, and `filePath` (all required). The output includes `uri` (Kestra storage URI) and `size` (bytes).

`onelake.Upload` uploads a file to OneLake — set `workspaceId`, `itemId`, `filePath` (destination path, all required), and `from` (required, a `kestra://` URI). The output includes `uri` (the ABFSS URI of the uploaded file).

`onelake.Delete` deletes a file or folder from OneLake — set `workspaceId`, `itemId`, and `filePath` (all required). Set `recursive: true` to delete a folder recursively (default `false`).

### Warehouse

`warehouse.Query` runs a SQL query against a Fabric Warehouse — set `sqlEndpointId`, `warehouseId`, and `sql` (all required). Set `fetchType` (default `STORE`) to control output: `STORE` writes to internal storage and returns a `uri`, `FETCH` returns `rows`, `FETCH_ONE` returns a single `row`. The output also includes `size`.

### Data Engineering

`data.engineering.RunNotebook` runs a Fabric notebook — set `workspaceId` and `notebookId` (both required). By default `wait` is `true`; set `wait: false` for fire-and-forget. Control polling with `pollFrequency` (default 5 seconds) and `timeout` (default 1 hour). Pass runtime parameters via `parameters`. The output includes `jobInstanceId` and `status`.

`data.engineering.RunSparkJob` runs a Spark job definition — set `workspaceId` and `sparkJobDefinitionId` (both required). Same `wait`, `pollFrequency`, `timeout`, and `parameters` as `RunNotebook`. The output includes `jobInstanceId` and `status`.

`data.engineering.RunPipeline` runs a Fabric Data Factory pipeline — set `workspaceId` and `pipelineId` (both required). Same `wait`, `pollFrequency`, `timeout`, and `parameters` options. The output includes `runId` and `status`.
