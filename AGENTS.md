# Kestra Microsoft Fabric Plugin

## What

Provides plugin components under `io.kestra.plugin.microsoft.fabric` for interacting with Microsoft Fabric services.

## Why

- What user problem does this solve? Teams orchestrating Microsoft Fabric workloads need a native Kestra plugin to trigger pipelines, run notebooks and Spark jobs, query Warehouses, manage OneLake files, and refresh Power BI semantic models — all within standard Kestra flows.
- Why would a team adopt this plugin in a workflow? It eliminates custom scripting and shell tasks, provides first-class output variables, and supports both service principal and managed identity authentication.
- What operational/business outcome does it enable? Teams can build end-to-end data pipelines that span Fabric and other systems, with full observability, retries, and error handling provided by Kestra.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin.microsoft.fabric`:

- `fabric` — root package; `AbstractFabricConnection` (base class for all tasks)
- `fabric.pipeline` — `Run` (trigger and poll Fabric Data Pipeline jobs)
- `fabric.onelake` — `Upload`, `Download`, `Delete` (ADLS Gen2 / OneLake file operations)
- `fabric.engineering` — `RunNotebook`, `RunSparkJob` (Fabric Notebook and Spark Job Definition execution)
- `fabric.warehouse` — `Query` (T-SQL queries against Fabric Warehouse via JDBC)
- `fabric.powerbi` — `RefreshDataset` (Power BI semantic model refresh)

Authentication: service principal (`tenantId` + `clientId` + `clientSecret`) or `DefaultAzureCredential` (managed identity / Azure CLI).

Infrastructure dependencies: none (uses Azure SDK and mssql-jdbc; no Docker Compose services required for unit tests).

Test infrastructure: WireMock for HTTP-based tasks (pipeline, engineering, powerbi); build-only assertions for OneLake and Warehouse tasks (live tests guarded by `FABRIC_TENANT_ID` env var).

### Key Plugin Classes

- `io.kestra.plugin.microsoft.fabric.AbstractFabricConnection`
- `io.kestra.plugin.microsoft.fabric.pipeline.Run`
- `io.kestra.plugin.microsoft.fabric.onelake.Upload`
- `io.kestra.plugin.microsoft.fabric.onelake.Download`
- `io.kestra.plugin.microsoft.fabric.onelake.Delete`
- `io.kestra.plugin.microsoft.fabric.engineering.RunNotebook`
- `io.kestra.plugin.microsoft.fabric.engineering.RunSparkJob`
- `io.kestra.plugin.microsoft.fabric.warehouse.Query`
- `io.kestra.plugin.microsoft.fabric.powerbi.RefreshDataset`

### Project Structure

```
plugin-microsoft-fabric/
├── src/main/java/io/kestra/plugin/microsoft/fabric/
│   ├── AbstractFabricConnection.java
│   ├── package-info.java
│   ├── pipeline/
│   │   ├── Run.java
│   │   └── package-info.java
│   ├── onelake/
│   │   ├── Upload.java
│   │   ├── Download.java
│   │   ├── Delete.java
│   │   └── package-info.java
│   ├── engineering/
│   │   ├── AbstractEngineering.java
│   │   ├── RunNotebook.java
│   │   ├── RunSparkJob.java
│   │   └── package-info.java
│   ├── warehouse/
│   │   ├── Query.java
│   │   └── package-info.java
│   └── powerbi/
│       ├── RefreshDataset.java
│       └── package-info.java
├── src/test/java/io/kestra/plugin/microsoft/fabric/
│   ├── pipeline/ — RunTest, TestableRun
│   ├── onelake/ — UploadTest, DownloadTest, DeleteTest
│   ├── engineering/ — RunNotebookTest, RunSparkJobTest, TestableRunNotebook, TestableRunSparkJob
│   ├── warehouse/ — QueryTest
│   └── powerbi/ — RefreshDatasetTest, TestableRefreshDataset
├── src/main/resources/
│   ├── icons/ — plugin-icon.svg + per-package SVGs
│   └── metadata/ — index.yaml + per-package YAMLs
├── build.gradle
└── README.md
```

## Local rules

- Base the wording on the implemented packages and classes, not on template README text.
- Use WireMock for HTTP-based task tests; guard live integration tests with `Assumptions.assumeTrue(System.getenv("FABRIC_TENANT_ID") != null)`.
- Test subclasses override `fabricApiBase()` and `resolveToken()` to point at WireMock and bypass Azure AD.

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
- https://learn.microsoft.com/en-us/rest/api/fabric/
