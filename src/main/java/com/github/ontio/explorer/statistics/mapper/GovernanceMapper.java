package com.github.ontio.explorer.statistics.mapper;

import com.github.ontio.explorer.statistics.model.GovernanceInfo;
import com.github.ontio.explorer.statistics.model.IncomeInfo;
import com.github.ontio.explorer.statistics.model.StakingInfo;
import org.springframework.stereotype.Repository;

import java.util.List;


@Repository
public interface GovernanceMapper {

    int removeGovernanceInfos();

    int saveGovernanceInfos(List<GovernanceInfo> infos);

    int getMaxIncomeCycle();

    int getIncomeInfoCount();

    void saveIncomeInfos(List<IncomeInfo> infos, int cycle);

    List<IncomeInfo> selectWithdrawableInfo(int i);

    List<IncomeInfo> selectNewNodeInfo(int i);

    int getMaxStakingInfoCycle();

    int getStakingInfoCountByCycle(int cycle);

    void saveStakingInfos(List<StakingInfo> infos, int cycle);
}
