package io.kestra.plugin.microsoft.fabric.powerbi;

import io.kestra.core.runners.RunContext;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/**
 * Test subclass that bypasses real Azure AD authentication and allows the Fabric API base URL
 * to be overridden to point at WireMock.
 */
@SuperBuilder
@Getter
@NoArgsConstructor
public class TestableRefreshDataset extends RefreshDataset {

    private String fabricApiBaseOverride;
    private String fakeToken;

    @Override
    protected String fabricApiBase() {
        return fabricApiBaseOverride != null ? fabricApiBaseOverride : super.fabricApiBase();
    }

    @Override
    protected String resolveToken(RunContext runContext) throws Exception {
        return fakeToken != null ? fakeToken : super.resolveToken(runContext);
    }
}
