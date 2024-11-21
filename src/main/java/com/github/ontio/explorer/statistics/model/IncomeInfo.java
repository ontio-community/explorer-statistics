package com.github.ontio.explorer.statistics.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class IncomeInfo {

    @JsonProperty("peer_pub_key")
    private String peerPubKey;

    private String address;

    @JsonProperty("ong_in_come")
    private String ongIncome;

    @JsonProperty("staking_pos")
    private Long stakingPos;
}
