package com.github.ontio.explorer.statistics.model.dto;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Builder;
import lombok.Data;

@Data
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class UpdateOffChainNodeInfoDto {

    private String nodeInfo;

    private String publicKey;

    private String signature;

    @Builder
    public UpdateOffChainNodeInfoDto(String nodeInfo, String publicKey, String signature) {
        this.nodeInfo = nodeInfo;
        this.publicKey = publicKey;
        this.signature = signature;
    }
}
