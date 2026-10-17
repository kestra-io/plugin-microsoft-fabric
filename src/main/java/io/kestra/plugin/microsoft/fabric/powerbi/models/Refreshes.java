package io.kestra.plugin.microsoft.fabric.powerbi.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record Refreshes(List<Refresh> value) {
}