package com.github.ontio.explorer.statistics.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class StakingInfo {

    @JsonProperty("peer_pub_key")
    private String peerPubKey;

    private String address;

    private Long amount;

    private String action;

    @JsonProperty("node_status")
    private Integer nodeStatus;

    @JsonProperty("block_height")
    private Integer blockHeight;

    @JsonProperty("tx_hash")
    private String txHash;
}
