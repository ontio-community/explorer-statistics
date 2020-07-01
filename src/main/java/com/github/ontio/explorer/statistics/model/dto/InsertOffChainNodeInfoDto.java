package com.github.ontio.explorer.statistics.model.dto;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Builder;
import lombok.Data;

@Data
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class InsertOffChainNodeInfoDto {

    private String name;

    private String publicKey;

    private String address;
}
