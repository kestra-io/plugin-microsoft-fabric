# Kestra Microsoft Fabric Plugin

## What

Provides plugin components under `io.kestra.plugin.microsoft.fabric` for Microsoft Fabric services, including OneLake storage, Warehouse SQL, Data Engineering jobs, and Power BI dataset refreshes.

## Architecture

This is a single-module Gradle plugin. Source packages currently include:

- `io.kestra.plugin.microsoft.fabric` — shared Fabric connection handling and root plugin metadata.
- `io.kestra.plugin.microsoft.fabric.onelake` — `Upload`, `Download`, and `Delete`.
- `io.kestra.plugin.microsoft.fabric.warehouse` — `Query`.
- `io.kestra.plugin.microsoft.fabric.engineering` — `RunPipeline`, `RunNotebook`, and `RunSparkJob`.
- `io.kestra.plugin.microsoft.fabric.powerbi` — `RefreshGroupDataset`, `AbstractPowerBi`, and the refresh response models.

Power BI deliberately uses its own credential request semantics: the token request targets the Microsoft identity platform and requests the Power BI API resource/scope (`https://analysis.windows.net/powerbi/api`), rather than reusing the Fabric REST API token scope.

## Compatibility

`io.kestra.plugin.microsoft.fabric.powerbi.RefreshGroupDataset` is the canonical task type. Its `@Plugin` annotation declares `io.kestra.plugin.powerbi.RefreshGroupDataset` as an alias for legacy flows. Do not reintroduce the removed `RefreshDataset` task; both names must not register as independent tasks for the same operation.

Keep the legacy `groupId`, `datasetId`, `wait`, `pollDuration`, `waitDuration`, credential fields, HTTP `options`, defaults, and output names compatible. By default `wait` is `false`, `pollDuration` is five seconds, and `waitDuration` is ten minutes. `notifyOption` is optional and must not change the request when omitted.

## Testing and build

- Use WireMock for Power BI REST requests and OAuth token acquisition; tests must not depend on real tenant credentials.
- Cover no-wait and wait behavior, successful and failed refreshes, missing `requestId`, bounded timeout, 429 polling throttling, and legacy alias metadata.
- Run `./gradlew test` and the repository's full checks before opening a pull request.
- Guard any future live Azure tests behind explicit environment-based assumptions.

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
- https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/refresh-dataset