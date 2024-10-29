package com.github.ontio.explorer.statistics.service;

import com.alibaba.fastjson.JSONObject;
import com.github.ontio.common.Address;
import com.github.ontio.common.Helper;
import com.github.ontio.core.asset.Sig;
import com.github.ontio.core.payload.InvokeCode;
import com.github.ontio.core.transaction.Transaction;
import com.github.ontio.crypto.Digest;
import com.github.ontio.explorer.statistics.common.ParamsConfig;
import com.github.ontio.explorer.statistics.common.Response;
import com.github.ontio.explorer.statistics.mapper.ConfigMapper;
import com.github.ontio.explorer.statistics.mapper.NodeInfoOffChainMapper;
import com.github.ontio.explorer.statistics.mapper.NodeInfoOnChainMapper;
import com.github.ontio.explorer.statistics.model.Config;
import com.github.ontio.explorer.statistics.model.NodeInfoOffChain;
import com.github.ontio.explorer.statistics.model.NodeInfoOnChain;
import com.github.ontio.explorer.statistics.model.dto.InsertOffChainNodeInfoDto;
import com.github.ontio.explorer.statistics.model.dto.UpdateOffChainNodeInfoDto;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Slf4j
@Service
@NoArgsConstructor
public class ConfigService {
    @Autowired
    private ParamsConfig paramsConfig;
    @Autowired
    private ConfigMapper configMapper;
    @Autowired
    private OntSdkService ontSdkService;
    @Autowired
    private NodeInfoOffChainMapper nodeInfoOffChainMapper;
    @Autowired
    private NodeInfoOnChainMapper nodeInfoOnChainMapper;
    @Autowired
    private ConsensusNodeService consensusNodeService;


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
        String name = insertOffChainNodeInfoDto.getName();
        String publicKey = insertOffChainNodeInfoDto.getPublicKey();
        if (!StringUtils.hasLength(publicKey)) {
            return new Response(61001, "Public key is blank", "");
        }
        if (!StringUtils.hasLength(name)) {
            name = "Node_" + publicKey.substring(0, 6);
        }
        String peerInfo = null;
        int i = 0;
        while (peerInfo == null && i < 6) {
            peerInfo = ontSdkService.getPeerInfo(publicKey);
            if (peerInfo == null) {
                i++;
                try {
                    Thread.sleep(500);
                } catch (Exception ignore) {
                }
            }
        }

        if (StringUtils.hasLength(peerInfo)) {
            JSONObject jsonObject = JSONObject.parseObject(peerInfo);
            int status = jsonObject.getIntValue("status");
            String address = jsonObject.getString("address");
            NodeInfoOffChain nodeInfoOffChain = new NodeInfoOffChain();
            nodeInfoOffChain.setPublicKey(publicKey);
            nodeInfoOffChain.setAddress(address);
            nodeInfoOffChain.setName(name);
            nodeInfoOffChain.setVerification(0);
            nodeInfoOffChain.setOntId("");
            if (status == 1 || status == 2) {
                nodeInfoOffChain.setNodeType(status);
            } else if (status == 3 || status == 5) {
                // 3为共识节点退出,5为黑名单,一般只会拉黑共识
                nodeInfoOffChain.setNodeType(2);
            } else {
                nodeInfoOffChain.setNodeType(1);
            }
            nodeInfoOffChain.setOpenFlag(true);
            try {
                nodeInfoOffChainMapper.insertSelective(nodeInfoOffChain);
                if (status == 1 || status == 2) {
                    consensusNodeService.updateConsensusNodeInfo();
                    consensusNodeService.updateNodeAnnualizedYield();
                }
            } catch (DuplicateKeyException e) {
                nodeInfoOffChainMapper.updateByPrimaryKeySelective(nodeInfoOffChain);
                NodeInfoOnChain nodeInfoOnChain = nodeInfoOnChainMapper.selectByPublicKey(publicKey);
                if (nodeInfoOnChain == null && (status == 1 || status == 2)) {
                    consensusNodeService.updateConsensusNodeInfo();
                    consensusNodeService.updateNodeAnnualizedYield();
                }
            }
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
        String nodePublicKey = nodeInfoOffChain.getPublicKey();
        String peerInfo = ontSdkService.getPeerInfo(nodePublicKey);
        if (!StringUtils.hasLength(peerInfo)) {
            return new Response(61003, "Node not found on chain", "");
        }
        JSONObject jsonObject = JSONObject.parseObject(peerInfo);
        int status = jsonObject.getIntValue("status");
        String address = jsonObject.getString("address");
        String stakeAddress = Address.addressFromPubKey(stakePublicKey).toBase58();
        if (!address.equals(stakeAddress)) {
            return new Response(62006, "Verify signature failed.", "");
        }
        nodeInfoOffChain.setAddress(address);
        if (status == 1 || status == 2) {
            nodeInfoOffChain.setNodeType(status);
        } else if (status == 3 || status == 5) {
            // 3为共识节点退出,5为黑名单,一般只会拉黑共识
            nodeInfoOffChain.setNodeType(2);
        } else {
            nodeInfoOffChain.setNodeType(1);
        }

        String name = nodeInfoOffChain.getName();
        if (!StringUtils.hasLength(name)) {
            name = "Node_" + nodePublicKey.substring(0, 6);
            nodeInfoOffChain.setName(name);
        }

        boolean forbidEditingName = paramsConfig.getForbidNodes().contains(nodePublicKey.toLowerCase());
        NodeInfoOffChain existNodeInfo = nodeInfoOffChainMapper.selectByPrimaryKey(nodePublicKey);
        if (existNodeInfo == null) {
            // insert
            if (forbidEditingName) {
                name = "Node_" + nodePublicKey.substring(0, 6);
                nodeInfoOffChain.setName(name);
                nodeInfoOffChain.setRegion("");
                nodeInfoOffChain.setIntroduction("");
            }
            nodeInfoOffChainMapper.insertSelective(nodeInfoOffChain);
        } else {
            if (forbidEditingName) {
                String oldName = existNodeInfo.getName();
                String oldRegion = existNodeInfo.getRegion();
                String oldIntroduction = existNodeInfo.getIntroduction();
                if (!oldName.equals(name)) {
                    return new Response(61004, "Unable to edit node name temporarily", "");
                }
                if (!oldRegion.equals(nodeInfoOffChain.getRegion())) {
                    return new Response(61004, "Unable to edit region temporarily", "");
                }
                if (!oldIntroduction.equals(nodeInfoOffChain.getIntroduction())) {
                    return new Response(61004, "Unable to edit description temporarily", "");
                }
            }
            // update
            nodeInfoOffChain.setVerification(null);
            nodeInfoOffChain.setFeeSharingRatio(null);
            nodeInfoOffChain.setOntologyHarbinger(null);
            nodeInfoOffChain.setOldNode(null);
            nodeInfoOffChain.setContactInfoVerified(null);
            nodeInfoOffChain.setBadActor(null);
            nodeInfoOffChain.setRisky(null);
            nodeInfoOffChainMapper.updateByPrimaryKeySelective(nodeInfoOffChain);
        }
        return new Response(0, "SUCCESS", "SUCCESS");
    }

    public Response updateOffChainInfoByLedger(UpdateOffChainNodeInfoDto updateOffChainNodeInfoDto) throws Exception {
        String nodeInfo = updateOffChainNodeInfoDto.getNodeInfo();
        String stakePublicKey = updateOffChainNodeInfoDto.getPublicKey();

        byte[] nodeInfoBytes = Helper.hexToBytes(nodeInfo);
        InvokeCode transaction = (InvokeCode) Transaction.deserializeFrom(nodeInfoBytes);
        String signature = Helper.toHexString(transaction.sigs[0].sigData[0]);
        byte[] payload = transaction.code;
        transaction.sigs = new Sig[0];
        String hex = transaction.toHexString();
        String tx = hex.substring(0, hex.length() - 2);
        byte[] data = Digest.hash256(Helper.hexToBytes(tx));

        boolean verify = ontSdkService.verifySignatureByPublicKey(stakePublicKey, data, signature);
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
        String nodePublicKey = nodeInfoOffChain.getPublicKey();
        String peerInfo = ontSdkService.getPeerInfo(nodePublicKey);
        if (!StringUtils.hasLength(peerInfo)) {
            return new Response(61003, "Node not found on chain", "");
        }
        JSONObject jsonObject = JSONObject.parseObject(peerInfo);
        int status = jsonObject.getIntValue("status");
        String address = jsonObject.getString("address");
        String stakeAddress = Address.addressFromPubKey(stakePublicKey).toBase58();
        if (!address.equals(stakeAddress)) {
            return new Response(62006, "Verify signature failed.", "");
        }
        nodeInfoOffChain.setAddress(address);
        if (status == 1 || status == 2) {
            nodeInfoOffChain.setNodeType(status);
        } else if (status == 3 || status == 5) {
            // 3为共识节点退出,5为黑名单,一般只会拉黑共识
            nodeInfoOffChain.setNodeType(2);
        } else {
            nodeInfoOffChain.setNodeType(1);
        }

        String name = nodeInfoOffChain.getName();
        if (!StringUtils.hasLength(name)) {
            name = "Node_" + nodePublicKey.substring(0, 6);
            nodeInfoOffChain.setName(name);
        }

        boolean forbidEditingName = paramsConfig.getForbidNodes().contains(nodePublicKey.toLowerCase());
        NodeInfoOffChain existNodeInfo = nodeInfoOffChainMapper.selectByPrimaryKey(nodePublicKey);
        if (existNodeInfo == null) {
            // insert
            if (forbidEditingName) {
                name = "Node_" + nodePublicKey.substring(0, 6);
                nodeInfoOffChain.setName(name);
                nodeInfoOffChain.setRegion("");
                nodeInfoOffChain.setIntroduction("");
            }
            nodeInfoOffChainMapper.insertSelective(nodeInfoOffChain);
        } else {
            if (forbidEditingName) {
                String oldName = existNodeInfo.getName();
                String oldRegion = existNodeInfo.getRegion();
                String oldIntroduction = existNodeInfo.getIntroduction();
                if (!oldName.equals(name)) {
                    return new Response(61004, "Unable to edit node name temporarily", "");
                }
                if (!oldRegion.equals(nodeInfoOffChain.getRegion())) {
                    return new Response(61004, "Unable to edit region temporarily", "");
                }
                if (!oldIntroduction.equals(nodeInfoOffChain.getIntroduction())) {
                    return new Response(61004, "Unable to edit description temporarily", "");
                }
            }
            // update
            nodeInfoOffChain.setVerification(null);
            nodeInfoOffChain.setFeeSharingRatio(null);
            nodeInfoOffChain.setOntologyHarbinger(null);
            nodeInfoOffChain.setOldNode(null);
            nodeInfoOffChain.setContactInfoVerified(null);
            nodeInfoOffChain.setBadActor(null);
            nodeInfoOffChain.setRisky(null);
            nodeInfoOffChainMapper.updateByPrimaryKeySelective(nodeInfoOffChain);
        }
        return new Response(0, "SUCCESS", "SUCCESS");
    }
}
