package com.github.ontio.explorer.statistics;

import com.alibaba.fastjson.JSON;
import com.github.ontio.explorer.statistics.aggregate.model.AddressAggregate;
import com.github.ontio.explorer.statistics.model.NodeInfoOnChain;
import com.github.ontio.explorer.statistics.service.ConsensusNodeService;
import com.github.ontio.explorer.statistics.task.DailyInfoSchedule;
import com.github.ontio.explorer.statistics.task.NodeSchedule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

//@Ignore
@RunWith(SpringRunner.class)
@SpringBootTest
public class ExplorerStatisticsApplicationTests {

    @Autowired
    DailyInfoSchedule dailyInfoSchedule;

    @Autowired
    NodeSchedule nodeSchedule;

    @Autowired
    ConsensusNodeService consensusNodeService;

    @Test
    public void testUpdateDailyInfo() {
        dailyInfoSchedule.updateDailyInfo();
    }

    @Test
    public void testUpdateApprovedContractInfo() {
        dailyInfoSchedule.updateApprovedContractInfo();
    }

    @Test
    public void testUpdateCandidateNodeINfo() {
        nodeSchedule.updateNodeInfo();
    }

    @Test
    public void testSyncNodeInfoOffChain() {
        List<NodeInfoOnChain> nodes = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            NodeInfoOnChain nodeInfoOnChain = new NodeInfoOnChain();
            nodeInfoOnChain.setAddress("testSyncNodeInfoOffChain");
            nodeInfoOnChain.setPublicKey("ppppppuuuuuuubbbbbbbkkkkkkk" + i);
            nodeInfoOnChain.setName("name" + i);
            nodeInfoOnChain.setNodeRank(i);
            nodeInfoOnChain.setCurrentStake(0L);
            nodeInfoOnChain.setProgress("0");
            nodeInfoOnChain.setDetailUrl("0");
            nodeInfoOnChain.setStatus(1);
            nodeInfoOnChain.setInitPos(1L);
            nodeInfoOnChain.setTotalPos(1L);
            nodeInfoOnChain.setMaxAuthorize(1L);
            nodeInfoOnChain.setNodeProportion("0");
            nodeInfoOnChain.setCurrentStakePercentage("0");
            nodes.add(nodeInfoOnChain);
        }
//        consensusNodeService.updateNodesTable(nodes);
    }

    @Test
    public void testUpdateNodeOverviewHistory() {
//        consensusNodeService.maintainBlkRndHistory(0, 8334300L, 60000);
    }

    @Test
    public void newAddressAggregateKey() {
        String address = "1";
        String hash = "2";
        AddressAggregate.AddressAggregateKey addressAggregateKey = new AddressAggregate.AddressAggregateKey(address, hash, false);
        System.out.println(JSON.toJSONString(addressAggregateKey));
    }

}
