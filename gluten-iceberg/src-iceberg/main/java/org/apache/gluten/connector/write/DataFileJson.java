package org.apache.gluten.connector.write;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DataFileJson {
    @JsonProperty
    public String path;

    @JsonProperty
    public MetricsJson metrics;

    @JsonProperty
    public long fileSizeInBytes = -1L;

    public static class MetricsJson {
        @JsonProperty
        public long recordCount = -1L;
    }
}
