///*
// * Copyright (C) 2018 The ontology Authors
// * This file is part of The ontology library.
// * The ontology is free software: you can redistribute it and/or modify
// * it under the terms of the GNU Lesser General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// * The ontology is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU Lesser General Public License for more details.
// * You should have received a copy of the GNU Lesser General Public License
// * along with The ontology.  If not, see <http://www.gnu.org/licenses/>.
// */

package com.github.ontio.explorer.statistics.service;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ontio.core.governance.GovernanceView;
import com.github.ontio.core.governance.PeerPoolItem;
import com.github.ontio.explorer.statistics.common.ParamsConfig;
import com.github.ontio.explorer.statistics.mapper.*;
import com.github.ontio.explorer.statistics.model.*;
import com.github.ontio.explorer.statistics.util.HttpClientUtil;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

@Slf4j
@Service
@NoArgsConstructor
public class ConsensusNodeService {

    private ParamsConfig paramsConfig;

    private ObjectMapper objectMapper;

    private NodeOverviewMapper nodeOverviewMapper;

    private NodeRankChangeMapper nodeRankChangeMapper;

    private NodeInfoOnChainMapper nodeInfoOnChainMapper;

    private NodeRankHistoryMapper nodeRankHistoryMapper;

    private NodeInfoOffChainMapper nodeInfoOffChainMapper;

    private NodeOverviewHistoryMapper nodeOverviewHistoryMapper;

    private TxDetailMapper txDetailMapper;

    private NodeInspireMapper nodeInspireMapper;

    private OntSdkService ontSdkService;

    private StatisticsService statisticsService;

    private InspireCalculationParamsMapper inspireCalculationParamsMapper;

    private TxEventLogMapper txEventLogMapper;

    @Autowired
    public ConsensusNodeService(ParamsConfig paramsConfig,
                                ObjectMapper objectMapper,
                                OntSdkService ontSdkService,
                                NodeOverviewMapper nodeOverviewMapper,
                                NodeRankChangeMapper nodeRankChangeMapper,
                                NodeInfoOnChainMapper nodeInfoOnChainMapper,
                                NodeRankHistoryMapper nodeRankHistoryMapper,
                                NodeInfoOffChainMapper nodeInfoOffChainMapper,
                                NodeOverviewHistoryMapper nodeOverviewHistoryMapper,
                                TxDetailMapper txDetailMapper,
                                NodeInspireMapper nodeInspireMapper,
                                StatisticsService statisticsService,
                                InspireCalculationParamsMapper inspireCalculationParamsMapper,
                                TxEventLogMapper txEventLogMapper) {
        this.paramsConfig = paramsConfig;
        this.ontSdkService = ontSdkService;
        this.objectMapper = objectMapper;
        this.nodeOverviewMapper = nodeOverviewMapper;
        this.nodeRankChangeMapper = nodeRankChangeMapper;
        this.nodeInfoOnChainMapper = nodeInfoOnChainMapper;
        this.nodeRankHistoryMapper = nodeRankHistoryMapper;
        this.nodeInfoOffChainMapper = nodeInfoOffChainMapper;
        this.nodeOverviewHistoryMapper = nodeOverviewHistoryMapper;
        this.txDetailMapper = txDetailMapper;
        this.nodeInspireMapper = nodeInspireMapper;
        this.statisticsService = statisticsService;
        this.inspireCalculationParamsMapper = inspireCalculationParamsMapper;
        this.txEventLogMapper = txEventLogMapper;
    }

    public void updateBlockCountToNextRound() {
        long blockCntToNxtRound = getBlockCountToNextRound();
        if (blockCntToNxtRound < 0) {
            return;
        }
        try {
            nodeOverviewMapper.updateBlkCntToNxtRnd(blockCntToNxtRound);
            log.info("Updating block count to next round with value {}", blockCntToNxtRound);
        } catch (Exception e) {
            log.warn("Updating block count to next round with value {} failed: {}", blockCntToNxtRound, e.getMessage());
        }
        // update node round history
        long roundStartBlock = getRoundStartBlock();
        if (roundStartBlock < 0) {
            return;
        }
        int stakingChangeCount = ontSdkService.getStakingChangeCount();
        updateBlkRndHistory(roundStartBlock, stakingChangeCount);
    }

    private void updateBlkRndHistory(long roundStartBlock, int stakingChangeCount) {
        List<NodeOverviewHistory> historyList = nodeOverviewHistoryMapper.checkHistoryExist();
        int size = historyList.size();
        if (size < 10) {
            Long maintainEndBlk = roundStartBlock;
            maintainBlkRndHistory(size, maintainEndBlk, stakingChangeCount);
        }
        long roundEndBlock = roundStartBlock + stakingChangeCount - 1;
        NodeOverviewHistory overviewHistory = new NodeOverviewHistory();
        overviewHistory.setRndStartBlk(roundStartBlock);
        List<NodeOverviewHistory> list = nodeOverviewHistoryMapper.select(overviewHistory);
        if (CollectionUtils.isEmpty(list)) {
            int cycle = ontSdkService.getGovernanceView().view;
            int roundStartTime = ontSdkService.getBlockTimeByHeight((int) roundStartBlock);
            overviewHistory.setRndStartTime(roundStartTime);
            overviewHistory.setRndEndBlk(roundEndBlock);
            overviewHistory.setCycle(cycle);
            nodeOverviewHistoryMapper.updateRnkEndTime(roundStartBlock - 1, roundStartTime);
            nodeOverviewHistoryMapper.insertSelective(overviewHistory);

            // update node round ong supply
            statisticsService.updateTotalOngSupply();
        }
    }

    private void maintainBlkRndHistory(int size, long maintainEndBlk, int stakingChangeCount) {
        int loop = 10 - size;
        int currentCycle = ontSdkService.getGovernanceView().view;
        for (int i = 0; i < loop; i++) {
            int times = loop - i;
            long roundStartBlock = maintainEndBlk - stakingChangeCount * times;
            long roundEndBlock = roundStartBlock + stakingChangeCount - 1;
            int roundStartTime = ontSdkService.getBlockTimeByHeight((int) roundStartBlock);
            int roundEndTime = ontSdkService.getBlockTimeByHeight((int) roundEndBlock);

            // 获取周期数
            int cycle = currentCycle - times;
            NodeOverviewHistory lastHistory = new NodeOverviewHistory();
            lastHistory.setRndEndBlk(roundStartBlock - 1);
            lastHistory = nodeOverviewHistoryMapper.selectOne(lastHistory);
            if (lastHistory != null) {
                cycle = lastHistory.getCycle() + 1;
            }

            NodeOverviewHistory history = new NodeOverviewHistory();
            history.setRndStartBlk(roundStartBlock);
            history.setRndEndBlk(roundEndBlock);
            history.setRndStartTime(roundStartTime);
            history.setRndEndTime(roundEndTime);
            history.setCycle(cycle);
            nodeOverviewHistoryMapper.insertSelective(history);
        }
    }

    public void updateNodeRankChange() {
        try {
            Long rankChangeBlockHeight = nodeRankChangeMapper.selectRankChangeBlockHeight();
            long currentBlockHeight = ontSdkService.getBlockHeight();
            if (rankChangeBlockHeight != null && rankChangeBlockHeight >= currentBlockHeight) {
                log.info("Current block height is {}, rank change block height is {}, no need to update", currentBlockHeight, rankChangeBlockHeight);
                return;
            }

            long lastRoundBlockHeight = nodeRankHistoryMapper.selectCurrentRoundBlockHeight();
            List<NodeInfoOnChain> currentNodeInfoOnChain = nodeInfoOnChainMapper.selectAll();
            if (currentNodeInfoOnChain == null) {
                log.warn("Selecting current node rank in height {} failed", currentBlockHeight);
                return;
            }
            int result = nodeRankChangeMapper.deleteAll();
            log.warn("Delete {} records in node rank history", result);
            for (NodeInfoOnChain currentRoundNode : currentNodeInfoOnChain) {
                NodeRankHistory lastRoundNodeRank = nodeRankHistoryMapper.selectNodeRankHistoryByPublicKeyAndBlockHeight(currentRoundNode.getPublicKey(), lastRoundBlockHeight);
                int rankChange = 0;
                if (lastRoundNodeRank != null) {
                    rankChange = lastRoundNodeRank.getNodeRank() - currentRoundNode.getNodeRank();
                }
                NodeRankChange nodeRankChange = NodeRankChange.builder()
                        .name(currentRoundNode.getName())
                        .address(currentRoundNode.getAddress())
                        .rankChange(rankChange)
                        .publicKey(currentRoundNode.getPublicKey())
                        .changeBlockHeight(currentBlockHeight)
                        .build();
                nodeRankChangeMapper.insert(nodeRankChange);
            }
        } catch (Exception e) {
            log.warn("Updating node rank change failed: {}", e.getMessage());
        }
    }

    public void updateNodeRankHistory() {
        long currentRoundBlockHeight;
        try {
            currentRoundBlockHeight = nodeRankHistoryMapper.selectCurrentRoundBlockHeight();
        } catch (NullPointerException e) {
            initNodeRankHistory();
            return;
        }
        try {
            long blockHeight = ontSdkService.getBlockHeight();
            long nextRoundBlockHeight = currentRoundBlockHeight + paramsConfig.getMaxStakingChangeCount();
            if (nextRoundBlockHeight > blockHeight) {
                log.info("Current block height is {}, next round block height should be {} ", blockHeight, nextRoundBlockHeight);
                return;
            }
            updateNodeRankHistoryFromNodeInfoOnChain(nextRoundBlockHeight);
        } catch (Exception e) {
            log.warn("Updating node position history failed {}", e.getMessage());
        }
    }

    private void initNodeRankHistory() {
        GovernanceView view = ontSdkService.getGovernanceView();
        if (view == null) {
            log.warn("Getting governance view in consensus node service failed:");
            return;
        }
        long currentRoundBlockHeight = view.height - paramsConfig.getMaxStakingChangeCount();
        updateNodeRankHistoryFromNodeInfoOnChain(currentRoundBlockHeight);
    }

    private void updateNodeRankHistoryFromNodeInfoOnChain(long currentRoundBlockHeight) {
        log.info("Updating node position history from node info on chain task begin");
        List<NodeInfoOnChain> nodeInfoOnChainList = nodeInfoOnChainMapper.selectAll();
        List<NodeRankHistory> nodePositionHistoryList = new ArrayList<>();
        for (NodeInfoOnChain node : nodeInfoOnChainList) {
            nodePositionHistoryList.add(new NodeRankHistory(node, currentRoundBlockHeight));
        }
        try {
            nodeRankHistoryMapper.batchInsertSelective(nodePositionHistoryList);
            log.info("Updating node position history from node info on chain task end");
        } catch (Exception e) {
            log.info("Updating node position history from node info on chain task failed: {}", e.getMessage());
        }
    }

    private long getBlockCountToNextRound() {
        GovernanceView view = ontSdkService.getGovernanceView();
        if (view == null) {
            log.warn("Getting governance view in consensus node service failed:");
            return -1;
        }
        long blockHeight = ontSdkService.getBlockHeight();
        return paramsConfig.getMaxStakingChangeCount() - (blockHeight - view.height);
    }

    private long getRoundStartBlock() {
        GovernanceView view = ontSdkService.getGovernanceView();
        if (view == null) {
            log.warn("Getting governance view in consensus node service failed:");
            return -1;
        }
        return view.height;
    }

    public void updateConsensusNodeInfo() {
        Map peerPool = getPeerPool();
        List<NodeInfoOnChain> nodes = getNodesWithAttributes(peerPool);
        nodes.sort((v1, v2) -> Long.compare(v2.getInitPos() + v2.getTotalPos(), v1.getInitPos() + v1.getTotalPos()));
        List<NodeInfoOnChain> nodeInfos = calcNodeInfo(nodes);
        nodes = matchNodeName(nodeInfos);
        updateNodesTable(nodes);
    }

    private Map getPeerPool() {
        try {
            return ontSdkService.getSdk().nativevm().governance().getPeerPoolMap();
        } catch (Exception e) {
            log.error("Get peer pool map failed: {}", e.getMessage());
            ontSdkService.switchSyncNode();
            return getPeerPool();
        }
    }

    private List<NodeInfoOnChain> getNodesWithAttributes(Map peerPool) {
        List<NodeInfoOnChain> nodes = new ArrayList<>();
        for (Object obj : peerPool.values()) {
            PeerPoolItem item = (PeerPoolItem) obj;
            // candidate nodes and consensus nodes
            if (item.status != 1 && item.status != 2) {
                continue;
            }
            HashMap<String, Object> attribute = getAttributes(item.peerPubkey);
            NodeInfoOnChain node = new NodeInfoOnChain(item);
            node.setMaxAuthorize(Long.parseLong(attribute.get("maxAuthorize").toString()));
            node.setNodeProportion((100 - (int) attribute.get("t1PeerCost")) + "%");
            node.setUserProportion((100 - (int) attribute.get("t1StakeCost")) + "%");
            nodes.add(node);
        }
        return nodes;
    }

    private HashMap<String, Object> getAttributes(String pubKey) {
        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {
        };
        try {
            String result = ontSdkService.getSdk().nativevm().governance().getPeerAttributes(pubKey);
            return objectMapper.readValue(result, typeRef);
        } catch (Exception e) {
            log.error("Getting {}'s peer attributes failed: {}", pubKey, e.getMessage());
            ontSdkService.switchSyncNode();
            return getAttributes(pubKey);
        }
    }

    private List<NodeInfoOnChain> calcNodeInfo(List<NodeInfoOnChain> nodes) {
        for (int i = 0; i < nodes.size(); i++) {
            NodeInfoOnChain node = nodes.get(i);
            node.setNodeRank(i + 1);
            BigDecimal currentPos = new BigDecimal(node.getInitPos()).add(new BigDecimal(node.getTotalPos()));
            BigDecimal targetPos = new BigDecimal(node.getInitPos()).add(new BigDecimal(node.getMaxAuthorize()));
            node.setCurrentStake(currentPos.longValue());
            node.setProgress(currentPos.multiply(new BigDecimal(100)).divide(targetPos, 2, RoundingMode.DOWN) + "%");
            node.setDetailUrl(paramsConfig.getConsensusNodeDetailUrl() + node.getPublicKey());
            BigDecimal percent = new BigDecimal(node.getCurrentStake()).multiply(new BigDecimal(100)).divide(new BigDecimal(1000000000), 4, RoundingMode.HALF_UP);
            node.setCurrentStakePercentage(percent.toPlainString().concat("%"));
            nodes.set(i, node);
        }
        return nodes;
    }

    private void updateNodesTable(List<NodeInfoOnChain> nodes) {
        if (nodes.size() == 0) {
            log.warn("Updating NodeInfoOnchain table failed, nodes list is empty.");
            return;
        }
        int result = nodeInfoOnChainMapper.deleteAll();
        log.info("Updating NodeInfoOnchain table: delete {} nodes info.", result);
        try {
            result = nodeInfoOnChainMapper.batchInsert(nodes);
            log.info("Updating tbl_node_info_on_chain: insert {} nodes info.", result);
        } catch (Exception e) {
            log.error("Inserting {} into tbl_node_info_on_chain failed.", nodes.toString());
            log.error("Updating tbl_node_info_on_chain failed: {}", e.getMessage());
        }
        // sync tbl_node_info_off_chain
        try {
            result = nodeInfoOffChainMapper.syncWithOnChainNodes();
            log.info("sync tbl_node_info_off_chain: insert {} nodes info.", result);
        } catch (Exception e) {
            log.error("Updating tbl_node_info_off_chain failed: {}", e.getMessage());
        }
    }

    private List<NodeInfoOnChain> matchNodeName(List<NodeInfoOnChain> nodeInfos) {
        int i = 0;
        for (NodeInfoOnChain info : nodeInfos) {
            try {
                String name = nodeInfoOffChainMapper.selectNameByPublicKey(info.getPublicKey());
                if (name == null) {
                    name = "";
                    log.warn("Selecting name by public key {} failed.", info.getPublicKey());
                }
                info.setName(name);
                nodeInfos.set(i, info);
            } catch (Exception e) {
                log.error("Matching node name failed: {}", info.getPublicKey());
                log.error(e.getMessage());
            }
            i++;
        }
        return nodeInfos;
    }

    public void updateNodeAnnualizedYield() throws Exception {
        List<NodeInfoOnChain> nodeInfoOnChains = nodeInfoOnChainMapper.selectAll();
        if (CollectionUtils.isEmpty(nodeInfoOnChains)) {
            return;
        }
        int preConsensusCount = ontSdkService.getPreConsensusCount();
        List<String> addressList = paramsConfig.getNodeFoundationAddress();
        List<String> nodeFoundationPublicKeys = paramsConfig.getNodeFoundationPublicKeys();
        List<BigDecimal> fpFuList = new ArrayList<>();
        List<NodeInfoOnChain> consensusNodes = new ArrayList<>();
        List<NodeInfoOnChain> candidateNodes = new ArrayList<>();
        Long top49Stake = 0L;
        BigDecimal totalSr = BigDecimal.ZERO;
        BigDecimal totalFuFp = BigDecimal.ZERO;
        // filter consensus and candidate node
        for (int i = 0; i < nodeInfoOnChains.size(); i++) {
            NodeInfoOnChain nodeInfoOnChain = nodeInfoOnChains.get(i);
            Integer status = nodeInfoOnChain.getStatus();
            String publicKey = nodeInfoOnChain.getPublicKey();
            if (i < preConsensusCount) {
                consensusNodes.add(nodeInfoOnChain);
            } else {
                candidateNodes.add(nodeInfoOnChain);
            }
            if (nodeFoundationPublicKeys.contains(publicKey)) {
                Long fu = 0L;
                Long currentStake = nodeInfoOnChain.getCurrentStake();
                Long fp = nodeInfoOnChain.getInitPos();
                String nodeProportion = nodeInfoOnChain.getNodeProportion().replace("%", "");
                BigDecimal proportion = new BigDecimal(nodeProportion).divide(new BigDecimal(100), 2, BigDecimal.ROUND_HALF_UP);
                for (String address : addressList) {
                    String authorizeInfo = ontSdkService.getAuthorizeInfo(nodeInfoOnChain.getPublicKey(), address);
                    Long consensusPos = 0L;
                    if (!StringUtils.isEmpty(authorizeInfo)) {
                        consensusPos = JSONObject.parseObject(authorizeInfo).getLong("consensusPos");
                    }
                    fu += consensusPos;
                }
                BigDecimal fuFp = new BigDecimal(fp).add(new BigDecimal(fu));
                fpFuList.add(fuFp);
                BigDecimal decimal1 = proportion.multiply(new BigDecimal(fu * currentStake)).divide(new BigDecimal(currentStake - fp), 12, BigDecimal.ROUND_HALF_UP);
                BigDecimal subtract = new BigDecimal(1).subtract(proportion);
                BigDecimal decimal2 = new BigDecimal(fp).multiply(subtract);
                BigDecimal sr = decimal1.add(decimal2);
                totalSr = totalSr.add(sr);
                totalFuFp = totalFuFp.add(fuFp);
            }
            if (i < 49) {
                Long currentStake = nodeInfoOnChain.getCurrentStake();
                top49Stake += currentStake;
            }
        }

        // Top 49 节点的质押总和
        BigDecimal topStake = new BigDecimal(top49Stake);

        // 第一轮
        BigDecimal first = new BigDecimal(10000000).divide(topStake, 12, BigDecimal.ROUND_HALF_UP);
        // 第二轮
        BigDecimal subtract = topStake.subtract(totalFuFp);
        BigDecimal second = totalSr.divide(subtract, 12, BigDecimal.ROUND_HALF_UP);

        //  候选节点的质押总和
        BigDecimal candidateTotalStake = getTotalStake(candidateNodes);

        BigDecimal consensusTotalStake = getTotalStake(consensusNodes);
        BigDecimal consensusCount = new BigDecimal(consensusNodes.size());
        //  共识节点的平均质押量
        BigDecimal consensusAverageStake = consensusTotalStake.divide(consensusCount, 12, BigDecimal.ROUND_HALF_UP);

        // A 为所有共识节点的激励系数总和
        Map<String, BigDecimal> consensusInspireMap = new HashMap<>();
        BigDecimal totalConsensusInspire = getConsensusInspire(consensusAverageStake, consensusInspireMap, consensusNodes);

        // 一年释放的 ONG 总量
        BigDecimal releaseOng = new BigDecimal(365 * 24 * 60 * 60).multiply(new BigDecimal(paramsConfig.releaseOngRatio));

        // 预测一年累积的手续费总量
        BigDecimal commission = BigDecimal.ZERO;
        BigDecimal lastMonthCommission = BigDecimal.ZERO;
        int now = (int) (System.currentTimeMillis() / 1000);
        int lastMonth = now - (30 * 24 * 60 * 60);
        // 获取激活时间
//        int activeTime = paramsConfig.getInspireActiveTime();
//        if (lastMonth < activeTime) {
//            if (now < activeTime) {
//                BigDecimal fee = txDetailMapper.findFeeAmountOneMonth(now, lastMonth);
//                lastMonthCommission = fee.multiply(new BigDecimal(5));
//            } else {
//                BigDecimal fee1 = txDetailMapper.findFeeAmountOneMonth(now, activeTime);
//                BigDecimal fee2 = txDetailMapper.findFeeAmountOneMonth(activeTime, lastMonth);
//                BigDecimal multiply = fee2.multiply(new BigDecimal(5));
//                lastMonthCommission = fee1.add(multiply);
//            }
//        } else {
//            lastMonthCommission = txDetailMapper.findFeeAmountOneMonth(now, lastMonth);
//        }
        lastMonthCommission = txEventLogMapper.findFeeAmountOneMonth(now, lastMonth);
        commission = lastMonthCommission.multiply(new BigDecimal(365)).divide(new BigDecimal(30), 12, BigDecimal.ROUND_HALF_UP);


        Map<String, Object> params = new HashMap<>();
        params.put("token", "ong");
        params.put("fiat", "USD");
        String ongResp = HttpClientUtil.getRequest(paramsConfig.getExplorerUrl() + "tokens/prices", params, new HashMap<>());
        String ongPrice = JSONObject.parseObject(ongResp).getJSONObject("result").getJSONObject("prices").getJSONObject("USD").getString("price");

        params.put("token", "ont");
        String ontResp = HttpClientUtil.getRequest(paramsConfig.getExplorerUrl() + "tokens/prices", params, new HashMap<>());
        String ontPrice = JSONObject.parseObject(ontResp).getJSONObject("result").getJSONObject("prices").getJSONObject("USD").getString("price");

        BigDecimal ong = new BigDecimal(ongPrice).setScale(12, BigDecimal.ROUND_HALF_UP);
        BigDecimal ont = new BigDecimal(ontPrice).setScale(12, BigDecimal.ROUND_HALF_UP);

        List<InspireCalculationParams> inspireCalculationParams = inspireCalculationParamsMapper.selectAll();
        if (CollectionUtils.isEmpty(inspireCalculationParams)) {
            InspireCalculationParams calculationParams = new InspireCalculationParams();
            calculationParams.setTotalFpFu(totalFuFp);
            calculationParams.setTotalSr(totalSr);
            calculationParams.setGasFee(commission);
            calculationParams.setOntPrice(ont);
            calculationParams.setOngPrice(ong);
            inspireCalculationParamsMapper.insertSelective(calculationParams);
        } else {
            InspireCalculationParams calculationParams = inspireCalculationParams.get(0);
            calculationParams.setTotalFpFu(totalFuFp);
            calculationParams.setTotalSr(totalSr);
            calculationParams.setGasFee(commission);
            calculationParams.setOntPrice(ont);
            calculationParams.setOngPrice(ong);
            inspireCalculationParamsMapper.updateByPrimaryKeySelective(calculationParams);
        }

        // 节点的收益计算
        List<NodeInspire> nodeInspireList = new ArrayList<>();
        BigDecimal oneHundred = new BigDecimal(100);
        for (int i = 0; i < nodeInfoOnChains.size(); i++) {
            NodeInspire nodeInspire = new NodeInspire();
            BigDecimal finalReleaseOng = BigDecimal.ZERO;
            BigDecimal finalCommission = BigDecimal.ZERO;
            BigDecimal foundationInspire = BigDecimal.ZERO;
            BigDecimal userFoundationInspire = BigDecimal.ZERO;

            NodeInfoOnChain nodeInfoOnChain = nodeInfoOnChains.get(i);
            String publicKey = nodeInfoOnChain.getPublicKey();
            Integer status = nodeInfoOnChain.getStatus();
            String initProportion = nodeInfoOnChain.getNodeProportion().replace("%", "");
            String stakeProportion = nodeInfoOnChain.getUserProportion().replace("%", "");
            BigDecimal initUserProportion = new BigDecimal(initProportion).divide(new BigDecimal(100), 2, BigDecimal.ROUND_HALF_UP);
            BigDecimal initNodeProportion = new BigDecimal(1).subtract(initUserProportion);
            BigDecimal stakeUserProportion = new BigDecimal(stakeProportion).divide(new BigDecimal(100), 2, BigDecimal.ROUND_HALF_UP);
            BigDecimal stakeNodeProportion = new BigDecimal(1).subtract(stakeUserProportion);
            BigDecimal currentStake = new BigDecimal(nodeInfoOnChain.getCurrentStake());
            BigDecimal nodeStake = new BigDecimal(nodeInfoOnChain.getInitPos());
            BigDecimal totalPos = new BigDecimal(nodeInfoOnChain.getTotalPos());
            BigDecimal userStake = new BigDecimal(nodeInfoOnChain.getTotalPos());

            if (i < preConsensusCount) {
                BigDecimal consensusInspire = consensusInspireMap.get(publicKey);
                // 共识节点手续费和释放的 ONG
                finalReleaseOng = getReleaseAndCommissionOng(consensusInspire, releaseOng, totalConsensusInspire);
                finalCommission = getReleaseAndCommissionOng(consensusInspire, commission, totalConsensusInspire);
            } else {
                // 候选节点手续费和释放的 ONG
                finalReleaseOng = getReleaseAndCommissionOng(currentStake, releaseOng, candidateTotalStake);
                finalCommission = getReleaseAndCommissionOng(currentStake, commission, candidateTotalStake);
            }
            if (nodeFoundationPublicKeys.contains(publicKey)) {
                BigDecimal fp = new BigDecimal(nodeInfoOnChain.getInitPos());
                BigDecimal siSubFp = currentStake.subtract(fp);
                foundationInspire = first.multiply(siSubFp).multiply(initNodeProportion);
                // 用户收益
                BigDecimal fpFu = fpFuList.get(i);
                userStake = currentStake.subtract(fpFu);
                BigDecimal siPb = currentStake.multiply(initUserProportion);
                BigDecimal add = siPb.divide(siSubFp, 12, BigDecimal.ROUND_HALF_UP).add(second);
                userFoundationInspire = first.multiply(userStake).multiply(add);
            } else if (i < 49) {
                foundationInspire = first.multiply(currentStake).multiply(new BigDecimal(1).add(second));
            }

            BigDecimal initPercent = nodeStake.divide(currentStake, 12, BigDecimal.ROUND_HALF_UP);
            BigDecimal stakePercent = totalPos.divide(currentStake, 12, BigDecimal.ROUND_HALF_UP);

            BigDecimal initPartFinalReleaseOng = finalReleaseOng.multiply(initPercent);
            BigDecimal stakePartFinalReleaseOng = finalReleaseOng.multiply(stakePercent);

            BigDecimal finalUserReleaseOng = (initPartFinalReleaseOng.multiply(initUserProportion)).add((stakePartFinalReleaseOng.multiply(stakeUserProportion)));
            BigDecimal finalNodeReleaseOng = (initPartFinalReleaseOng.multiply(initNodeProportion)).add((stakePartFinalReleaseOng.multiply(stakeNodeProportion)));

            BigDecimal initPartFinalCommission = finalCommission.multiply(initPercent);
            BigDecimal stakePartFinalCommission = finalCommission.multiply(stakePercent);

            BigDecimal finalUserCommission = (initPartFinalCommission.multiply(initUserProportion)).add((stakePartFinalCommission.multiply(stakeUserProportion)));
            BigDecimal finalNodeCommission = (initPartFinalCommission.multiply(initNodeProportion)).add((stakePartFinalCommission.multiply(stakeNodeProportion)));

            if (totalPos.compareTo(BigDecimal.ZERO) == 0) {
                totalPos = new BigDecimal(paramsConfig.insteadZeroPos);
            }
            if (userStake.compareTo(BigDecimal.ZERO) == 0) {
                userStake = new BigDecimal(paramsConfig.insteadZeroPos);
            }

            BigDecimal nodeStakeUsd = nodeStake.multiply(ont);
            BigDecimal totalPosUsd = totalPos.multiply(ont);
            BigDecimal userStakeUsd = userStake.multiply(ont);
            BigDecimal nodeReleaseUsd = finalNodeReleaseOng.multiply(ong);
            BigDecimal nodeCommissionUsd = finalNodeCommission.multiply(ong);
            BigDecimal nodeFoundationUsd = foundationInspire.multiply(ong);
            BigDecimal userReleaseUsd = finalUserReleaseOng.multiply(ong);
            BigDecimal userCommissionUsd = finalUserCommission.multiply(ong);
            BigDecimal userFoundationUsd = userFoundationInspire.multiply(ong);

            nodeInspire.setPublicKey(nodeInfoOnChain.getPublicKey());
            nodeInspire.setAddress(nodeInfoOnChain.getAddress());
            nodeInspire.setName(nodeInfoOnChain.getName());
            nodeInspire.setStatus(status);
            nodeInspire.setCurrentStake(nodeInfoOnChain.getCurrentStake());
            nodeInspire.setInitPos(nodeInfoOnChain.getInitPos());
            nodeInspire.setTotalPos(nodeInfoOnChain.getTotalPos());
            nodeInspire.setNodeReleasedOngIncentive(finalNodeReleaseOng.longValue());
            nodeInspire.setNodeGasFeeIncentive(finalNodeCommission.longValue());
            nodeInspire.setNodeFoundationBonusIncentive(foundationInspire.longValue());

            nodeInspire.setNodeReleasedOngIncentiveRate(nodeReleaseUsd.divide(nodeStakeUsd, 12, BigDecimal.ROUND_HALF_UP).multiply(oneHundred).setScale(2, BigDecimal.ROUND_HALF_UP).toPlainString() + "%");
            nodeInspire.setNodeGasFeeIncentiveRate(nodeCommissionUsd.divide(nodeStakeUsd, 12, BigDecimal.ROUND_HALF_UP).multiply(oneHundred).setScale(2, BigDecimal.ROUND_HALF_UP).toPlainString() + "%");
            nodeInspire.setNodeFoundationBonusIncentiveRate(nodeFoundationUsd.divide(nodeStakeUsd, 12, BigDecimal.ROUND_HALF_UP).multiply(oneHundred).setScale(2, BigDecimal.ROUND_HALF_UP).toPlainString() + "%");

            Long maxAuthorize = nodeInfoOnChain.getMaxAuthorize();
            if (maxAuthorize == 0 && nodeInfoOnChain.getTotalPos() == 0) {
                nodeInspire.setUserReleasedOngIncentive(0L);
                nodeInspire.setUserGasFeeIncentive(0L);
                nodeInspire.setUserFoundationBonusIncentive(0L);
                nodeInspire.setUserReleasedOngIncentiveRate("0.00%");
                nodeInspire.setUserGasFeeIncentiveRate("0.00%");
                nodeInspire.setUserFoundationBonusIncentiveRate("0.00%");
            } else {
                nodeInspire.setUserReleasedOngIncentive(finalUserReleaseOng.longValue());
                nodeInspire.setUserGasFeeIncentive(finalUserCommission.longValue());
                nodeInspire.setUserFoundationBonusIncentive(userFoundationInspire.longValue());
                nodeInspire.setUserReleasedOngIncentiveRate(userReleaseUsd.divide(totalPosUsd, 12, BigDecimal.ROUND_HALF_UP).multiply(oneHundred).setScale(2, BigDecimal.ROUND_HALF_UP).toPlainString() + "%");
                nodeInspire.setUserGasFeeIncentiveRate(userCommissionUsd.divide(totalPosUsd, 12, BigDecimal.ROUND_HALF_UP).multiply(oneHundred).setScale(2, BigDecimal.ROUND_HALF_UP).toPlainString() + "%");
                nodeInspire.setUserFoundationBonusIncentiveRate(userFoundationUsd.divide(userStakeUsd, 12, BigDecimal.ROUND_HALF_UP).multiply(oneHundred).setScale(2, BigDecimal.ROUND_HALF_UP).toPlainString() + "%");
            }

            nodeInspireList.add(nodeInspire);
        }

        // update node inspire
        nodeInspireMapper.deleteAll();
        nodeInspireMapper.batchInsert(nodeInspireList);
    }

    private BigDecimal getReleaseAndCommissionOng(BigDecimal value, BigDecimal ong, BigDecimal totalConsensusInspire) {
        return new BigDecimal(0.5).multiply(ong).multiply(value).divide(totalConsensusInspire, 12, BigDecimal.ROUND_HALF_UP);
    }

    private BigDecimal getConsensusInspire(BigDecimal consensusAverageStake, Map<String, BigDecimal> consensusInspireMap, List<NodeInfoOnChain> consensusNodes) {
        BigDecimal totalConsensusInspire = BigDecimal.ZERO;
        for (NodeInfoOnChain nodeInfoOnChain : consensusNodes) {
            Long currentStake = nodeInfoOnChain.getCurrentStake();
            BigDecimal xi = new BigDecimal(currentStake * 0.5).divide(consensusAverageStake, 12, BigDecimal.ROUND_HALF_UP);
            double pow = Math.pow(Math.E, (BigDecimal.ZERO.subtract(xi)).doubleValue());
            BigDecimal consensusInspire = xi.multiply(new BigDecimal(pow)).setScale(12, BigDecimal.ROUND_HALF_UP);
            consensusInspireMap.put(nodeInfoOnChain.getPublicKey(), consensusInspire);
            totalConsensusInspire = totalConsensusInspire.add(consensusInspire);
        }
        return totalConsensusInspire;
    }

    private BigDecimal getTotalStake(List<NodeInfoOnChain> nodes) {
        Long totalStake = 0L;
        for (NodeInfoOnChain node : nodes) {
            Long currentStake = node.getCurrentStake();
            totalStake += currentStake;
        }
        return new BigDecimal(totalStake);
    }
}
