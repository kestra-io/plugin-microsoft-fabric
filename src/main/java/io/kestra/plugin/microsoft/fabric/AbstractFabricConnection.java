package io.kestra.plugin.microsoft.fabric;

import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractFabricConnection extends Task {

    @Schema(title = "Tenant ID", description = "Azure Active Directory tenant ID for authentication")
    @PluginProperty(group = "connection")
    protected Property<String> tenantId;

    @Schema(title = "Client ID", description = "Service principal client (application) ID")
    @PluginProperty(group = "connection")
    protected Property<String> clientId;

    @Schema(
        title = "Client Secret",
        description = "Service principal client secret. When both clientId and clientSecret are provided, service principal authentication is used; otherwise DefaultAzureCredential is used."
    )
    @PluginProperty(group = "connection", secret = true)
    protected Property<String> clientSecret;

    protected TokenCredential credentials(RunContext runContext) throws Exception {
        var rTenantId = runContext.render(tenantId).as(String.class).orElse(null);
        var rClientId = runContext.render(clientId).as(String.class).orElse(null);
        var rClientSecret = runContext.render(clientSecret).as(String.class).orElse(null);

        if (rTenantId != null && rClientId != null && rClientSecret != null) {
            return new ClientSecretCredentialBuilder()
                .tenantId(rTenantId)
                .clientId(rClientId)
                .clientSecret(rClientSecret)
                .build();
        }
        return new DefaultAzureCredentialBuilder().build();
    }

    protected String bearerToken(RunContext runContext) throws Exception {
        var cred = credentials(runContext);
        var ctx = new TokenRequestContext().addScopes("https://api.fabric.microsoft.com/.default");
        return cred.getTokenSync(ctx).getToken();
    }
}
