package io.kestra.plugin.microsoft.fabric.powerbi;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/** Test adapter that points Power BI requests at WireMock. */
@SuperBuilder
@Getter
@NoArgsConstructor
public class TestableRefreshGroupDataset extends RefreshGroupDataset {
    private String loginUrlOverride;
    private String apiUrlOverride;

    @Override
    protected String loginUrl() {
        return loginUrlOverride != null ? loginUrlOverride : super.loginUrl();
    }

    @Override
    protected String apiUrl() {
        return apiUrlOverride != null ? apiUrlOverride : super.apiUrl();
    }
}