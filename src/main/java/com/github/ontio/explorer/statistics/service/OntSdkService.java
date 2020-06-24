package com.github.ontio.explorer.statistics.service;

import com.alibaba.fastjson.JSONObject;
import com.github.ontio.OntSdk;
import com.github.ontio.common.Address;
import com.github.ontio.common.Helper;
import com.github.ontio.core.block.Block;
import com.github.ontio.core.governance.Configuration;
import com.github.ontio.core.governance.GovernanceView;
import com.github.ontio.explorer.statistics.common.ParamsConfig;
import com.github.ontio.network.exception.ConnectorException;
import com.github.ontio.sdk.exception.SDKException;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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

    GovernanceView getGovernanceView() {
        try {
            return sdk.nativevm().governance().getGovernanceView();
        } catch (ConnectorException | IOException | SDKException e) {
            log.warn("Getting governance view failed: {}", e.getMessage());
            switchSyncNode();
            log.info("Getting governance view again");
            return getGovernanceView();
        }
    }

    int getStakingChangeCount() {
        try {
            Configuration configuration = sdk.nativevm().governance().getConfiguration();
            if (configuration == null) {
                log.warn("Getting native vm configuration failed: configuration is null");
                switchSyncNode();
                log.info("Try to get native vm configuration again");
                return getStakingChangeCount();
            }
            return configuration.MaxBlockChangeView;
        } catch (ConnectorException | IOException | SDKException e) {
            log.warn("Getting native vm configuration failed: {}", e.getMessage());
            switchSyncNode();
            log.info("Getting native vm configuration again");
            return getStakingChangeCount();
        }
    }

    int getBlockHeight() {
        try {
            return sdk.getRestful().getBlockHeight();
        } catch (ConnectorException | IOException | SDKException e) {
            log.warn("Getting block height failed: {}", e.getMessage());
            switchSyncNode();
            log.info("Getting block height again");
            return getBlockHeight();
        }
    }

    int getBlockTimeByHeight(int height) {
        try {
            Block block = sdk.getRestful().getBlock(height);
            return block.timestamp;
        } catch (ConnectorException | IOException | SDKException e) {
            log.warn("Getting block height failed: {}", e.getMessage());
            switchSyncNode();
            log.info("Getting block height again");
            return getBlockTimeByHeight(height);
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

    public String getAuthorizeInfo(String publicKey, String address) {
        try {
            Address addr = Address.decodeBase58(address);
            return sdk.nativevm().governance().getAuthorizeInfo(publicKey, addr);
        } catch (SDKException e) {
            log.warn("Getting authorize info failed: {}", e.getMessage());
            switchSyncNode();
            log.info("Getting authorize info again");
            return getAuthorizeInfo(publicKey, address);
        }
    }

}
