package com.github.ontio.explorer.statistics.service;

import com.alibaba.fastjson.JSONObject;
import com.github.ontio.OntSdk;
import com.github.ontio.account.Account;
import com.github.ontio.common.Address;
import com.github.ontio.common.Helper;
import com.github.ontio.core.block.Block;
import com.github.ontio.core.governance.Configuration;
import com.github.ontio.core.governance.GovernanceView;
import com.github.ontio.explorer.statistics.common.ParamsConfig;
import com.github.ontio.io.BinaryReader;
import com.github.ontio.network.exception.ConnectorException;
import com.github.ontio.sdk.exception.SDKException;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

@Data
@Slf4j
@Service
@NoArgsConstructor
public class OntSdkService {

    private OntSdk sdk;

    private int nodeCount;

    private AtomicInteger currentNodeIndex;

    private ParamsConfig paramsConfig;

    private final String contractAddress = "0000000000000000000000000000000000000007";

    @Autowired
    public OntSdkService(ParamsConfig paramsConfig) {
        this.paramsConfig = paramsConfig;
        this.nodeCount = paramsConfig.getHosts().size();
        this.currentNodeIndex = new AtomicInteger(0);
        this.sdk = OntSdk.getInstance();
        try {
            sdk.getRestful();
        } catch (SDKException e) {
            sdk.setRestful(paramsConfig.getHosts().get(this.currentNodeIndex.get()));
        }
    }

    GovernanceView getGovernanceView() throws Exception {
        try {
            return sdk.nativevm().governance().getGovernanceView();
        } catch (ConnectorException | IOException | SDKException e) {
            log.warn("Getting governance view failed: {}", e.getMessage());
            switchSyncNode();
//            log.info("Getting governance view again");
//            return getGovernanceView();
            throw e;
        }
    }

    int getStakingChangeCount() throws Exception {
        try {
            Configuration configuration = sdk.nativevm().governance().getConfiguration();
            if (configuration == null) {
                log.warn("Getting native vm configuration failed: configuration is null");
                switchSyncNode();
//                log.info("Try to get native vm configuration again");
//                return getStakingChangeCount();

            }
            return configuration.MaxBlockChangeView;
        } catch (ConnectorException | IOException e) {
            log.warn("Getting native vm configuration failed: {}", e.getMessage());
            switchSyncNode();
//            log.info("Getting native vm configuration again");
//            return getStakingChangeCount();
            throw e;
        }
    }

    int getBlockHeight() throws Exception{
        try {
            return sdk.getRestful().getBlockHeight();
        } catch (ConnectorException | IOException | SDKException e) {
            log.warn("Getting block height failed: {}", e.getMessage());
            switchSyncNode();
//            log.info("Getting block height again");
//            return getBlockHeight();
            throw e;
        }
    }

    int getBlockTimeByHeight(int height) throws Exception {
        try {
            Block block = sdk.getRestful().getBlock(height);
            return block.timestamp;
        } catch (ConnectorException | IOException | SDKException e) {
            log.warn("Getting block height failed: {}", e.getMessage());
            switchSyncNode();
//            log.info("Getting block height again");
//            return getBlockTimeByHeight(height);
            throw e;
        }
    }

    void switchSyncNode() {
        if (currentNodeIndex.get() >= nodeCount) {
            currentNodeIndex.set(0);
        }
        sdk.setRestful(paramsConfig.getHosts().get(currentNodeIndex.getAndAdd(1)));
        try {
            log.warn("Using node: {}", sdk.getRestful().toString());
        } catch (SDKException e) {
            log.warn("Getting REST URL failed: {}", e.getMessage());
        }
    }

    public String getAuthorizeInfo(String publicKey, String address) throws Exception {
        try {
            Address addr = Address.decodeBase58(address);
            return sdk.nativevm().governance().getAuthorizeInfo(publicKey, addr);
        } catch (SDKException e) {
            log.warn("Getting authorize info failed: {}", e.getMessage());
            switchSyncNode();
//            log.info("Getting authorize info again");
//            return getAuthorizeInfo(publicKey, address);
            throw e;
        }
    }

    /**
     * verify signature by publicKey
     *
     * @param publicKey
     * @param origData
     * @param signatureStr
     * @return
     */
    public boolean verifySignatureByPublicKey(String publicKey, byte[] origData, String signatureStr) throws Exception {
        Account account = new Account(false, Helper.hexToBytes(publicKey));
        Boolean verify = account.verifySignature(origData, Helper.hexToBytes(signatureStr));
        return verify;
    }

    public String getPeerInfo(String publicKey) throws Exception {
        String peerInfo = sdk.nativevm().governance().getPeerInfo(publicKey);
        return peerInfo;
    }

    public int getPreConsensusCount() throws Exception {
        try {
            Configuration preConfiguration = getPreConfiguration();
            return preConfiguration.K;
        } catch (Exception e) {
            log.warn("Getting authorize info failed: {}", e.getMessage());
            switchSyncNode();
//            log.info("Getting authorize info again");
//            return getPreConsensusCount();
            throw e;
        }
    }

    public Configuration getPreConfiguration() throws Exception {
        String res = sdk.getConnect().getStorage(Helper.reverse(contractAddress), Helper.toHexString("preConfig".getBytes()));
        if (res == null) {
            return null;
        }
        Configuration configuration = new Configuration();
        ByteArrayInputStream in = new ByteArrayInputStream(Helper.hexToBytes(res));
        BinaryReader reader = new BinaryReader(in);
        configuration.deserialize(reader);
        return configuration;
    }
}
