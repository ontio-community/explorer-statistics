package com.github.ontio.explorer.statistics.service;

import com.alibaba.fastjson.JSONObject;
import com.github.ontio.common.Helper;
import com.github.ontio.core.asset.Sig;
import com.github.ontio.core.payload.InvokeCode;
import com.github.ontio.core.transaction.Transaction;
import com.github.ontio.crypto.Digest;
import com.github.ontio.explorer.statistics.common.ParamsConfig;
import com.github.ontio.explorer.statistics.common.Response;
import com.github.ontio.explorer.statistics.mapper.ConfigMapper;
import com.github.ontio.explorer.statistics.mapper.NodeInfoOffChainMapper;
import com.github.ontio.explorer.statistics.model.Config;
import com.github.ontio.explorer.statistics.model.NodeInfoOffChain;
import com.github.ontio.explorer.statistics.model.dto.InsertOffChainNodeInfoDto;
import com.github.ontio.explorer.statistics.model.dto.UpdateOffChainNodeInfoDto;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Slf4j
@Service
@NoArgsConstructor
public class ConfigService {

    private ParamsConfig paramsConfig;

    private ConfigMapper configMapper;

    private OntSdkService ontSdkService;

    private NodeInfoOffChainMapper nodeInfoOffChainMapper;

    @Autowired
    public ConfigService(ParamsConfig paramsConfig, ConfigMapper configMapper, OntSdkService ontSdkService, NodeInfoOffChainMapper nodeInfoOffChainMapper) {
        this.paramsConfig = paramsConfig;
        this.configMapper = configMapper;
        this.ontSdkService = ontSdkService;
        this.nodeInfoOffChainMapper = nodeInfoOffChainMapper;
    }


    public String getMaxStakingChangeCount() {
        Config config = configMapper.selectByPrimaryKey(ParamsConfig.Field.maxStakingChangeCount);
        return config == null ? "" : config.getValue();
    }

    public String updateMaxStakingChangeCount() {
        int maxStakingChangeCount = ontSdkService.getStakingChangeCount();
        Config config = new Config(ParamsConfig.Field.maxStakingChangeCount, String.valueOf(maxStakingChangeCount));
        Config selectConfig = configMapper.selectByPrimaryKey(ParamsConfig.Field.maxStakingChangeCount);
        int result;
        if (selectConfig == null) {
            result = configMapper.insert(config);
        } else {
            result = configMapper.updateByPrimaryKeySelective(config);
        }
        if (result != 1) {
            log.warn("Updating max block change view to {} failed", config.getValue());
            return "";
        }
        log.info("Updating max block change view to {} success", config.getValue());
        paramsConfig.setMaxStakingChangeCount(maxStakingChangeCount);
        log.info("Max staking change count has been updated to {}", paramsConfig.getMaxStakingChangeCount());
        return config.getValue();
    }

    public Response insertOffChainInfo(InsertOffChainNodeInfoDto insertOffChainNodeInfoDto) throws Exception {
        String address = insertOffChainNodeInfoDto.getAddress();
        String name = insertOffChainNodeInfoDto.getName();
        String publicKey = insertOffChainNodeInfoDto.getPublicKey();
        if (StringUtils.isEmpty(publicKey)) {
            return new Response(61001, "Public key is blank", "");
        }
        String peerInfo = ontSdkService.getPeerInfo(publicKey);
        if (!StringUtils.isEmpty(peerInfo)) {
            NodeInfoOffChain nodeInfoOffChain = new NodeInfoOffChain();
            nodeInfoOffChain.setPublicKey(publicKey);
            nodeInfoOffChain.setAddress(address);
            nodeInfoOffChain.setName(name);
            nodeInfoOffChain.setVerification(0);
            nodeInfoOffChain.setOntId("");
            nodeInfoOffChain.setNodeType(1);
            nodeInfoOffChain.setOpenFlag(true);
            nodeInfoOffChainMapper.insertSelective(nodeInfoOffChain);
            return new Response(0, "SUCCESS", "SUCCESS");
        } else {
            return new Response(61003, "Node not found on chain", "");
        }

    }

    public Response updateOffChainInfoByPublicKey(UpdateOffChainNodeInfoDto updateOffChainNodeInfoDto) throws Exception {
        String nodeInfo = updateOffChainNodeInfoDto.getNodeInfo();
        String stakePublicKey = updateOffChainNodeInfoDto.getPublicKey();
        String signature = updateOffChainNodeInfoDto.getSignature();

        byte[] nodeInfoBytes = Helper.hexToBytes(nodeInfo);
        boolean verify = ontSdkService.verifySignatureByPublicKey(stakePublicKey, nodeInfoBytes, signature);
        if (!verify) {
            return new Response(62006, "Verify signature failed.", "");
        }
        String nodeInfoStr = new String(nodeInfoBytes, "UTF-8");
        NodeInfoOffChain nodeInfoOffChain = JSONObject.parseObject(nodeInfoStr, NodeInfoOffChain.class);

        String ontId = nodeInfoOffChain.getOntId();
        if (ontId == null) {
            nodeInfoOffChain.setOntId("");
        }
        nodeInfoOffChain.setNodeType(1);
        String nodePublicKey = nodeInfoOffChain.getPublicKey();
        String name = nodeInfoOffChainMapper.selectNameByPublicKey(nodePublicKey);
        if (StringUtils.isEmpty(name)) {
            // insert
            nodeInfoOffChainMapper.insertSelective(nodeInfoOffChain);
        } else {
            // update
            nodeInfoOffChainMapper.updateByPrimaryKeySelective(nodeInfoOffChain);
        }
        return new Response(0, "SUCCESS", "SUCCESS");
    }

    public Response updateOffChainInfoByLedger(UpdateOffChainNodeInfoDto updateOffChainNodeInfoDto) throws Exception {
        String nodeInfo = updateOffChainNodeInfoDto.getNodeInfo();
        String publicKey = updateOffChainNodeInfoDto.getPublicKey();

        byte[] nodeInfoBytes = Helper.hexToBytes(nodeInfo);
        InvokeCode transaction = (InvokeCode) Transaction.deserializeFrom(nodeInfoBytes);
        String signature = Helper.toHexString(transaction.sigs[0].sigData[0]);
        byte[] payload = transaction.code;
        transaction.sigs = new Sig[0];
        String hex = transaction.toHexString();
        String tx = hex.substring(0, hex.length() - 2);
        byte[] data = Digest.hash256(Helper.hexToBytes(tx));

        boolean verify = ontSdkService.verifySignatureByPublicKey(publicKey, data, signature);
        if (!verify) {
            return new Response(62006, "Verify signature failed.", "");
        }
        String nodeInfoStr = new String(payload, "UTF-8");
        log.info("ledger nodeInfoStr:{}", nodeInfoStr);
        NodeInfoOffChain nodeInfoOffChain = JSONObject.parseObject(nodeInfoStr, NodeInfoOffChain.class);

        String ontId = nodeInfoOffChain.getOntId();
        if (ontId == null) {
            nodeInfoOffChain.setOntId("");
        }
        nodeInfoOffChain.setNodeType(1);
        String nodePublicKey = nodeInfoOffChain.getPublicKey();
        String name = nodeInfoOffChainMapper.selectNameByPublicKey(nodePublicKey);
        if (StringUtils.isEmpty(name)) {
            // insert
            nodeInfoOffChainMapper.insertSelective(nodeInfoOffChain);
        } else {
            // update
            nodeInfoOffChainMapper.updateByPrimaryKeySelective(nodeInfoOffChain);
        }
        return new Response(0, "SUCCESS", "SUCCESS");
    }
}
