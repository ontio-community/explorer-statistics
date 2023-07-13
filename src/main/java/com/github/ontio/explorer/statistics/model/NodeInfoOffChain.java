package com.github.ontio.explorer.statistics.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tbl_node_info_off_chain")
public class NodeInfoOffChain {
    @Id
    @Column(name = "public_key")
    @GeneratedValue(generator = "JDBC")
    private String publicKey;

    private String name;

    private String address;

    @Column(name = "ont_id")
    private String ontId;

    @Column(name = "node_type")
    private Integer nodeType;

    private String introduction;

    @Column(name = "logo_url")
    private String logoUrl;

    private String region;

    private BigDecimal longitude;

    private BigDecimal latitude;

    private String ip;

    private String website;

    @Column(name = "social_media")
    private String socialMedia;

    private String telegram;

    private String twitter;

    private String facebook;

    @Column(name = "open_mail")
    private String openMail;

    @Column(name = "contact_mail")
    private String contactMail;

    @Column(name = "open_flag")
    private Boolean openFlag;

    private Integer verification;

    @Column(name = "fee_sharing_ratio")
    private Integer feeSharingRatio;

    @Column(name = "ontology_harbinger")
    private Integer ontologyHarbinger;

    @Column(name = "old_node")
    private Integer oldNode;

    @Column(name = "contact_info_verified")
    private Integer contactInfoVerified;

    @Column(name = "bad_actor")
    private Integer badActor;

    private Integer risky;
}