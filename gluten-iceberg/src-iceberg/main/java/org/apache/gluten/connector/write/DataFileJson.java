package org.apache.gluten.connector.write;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DataFileJson {
    @JsonProperty
    String filePath;

    @JsonProperty
    long recordCount = -1L;

    @JsonProperty
    long fileSizeInBytes = -1L;
}
