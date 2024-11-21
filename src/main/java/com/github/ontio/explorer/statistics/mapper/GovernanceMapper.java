package com.github.ontio.explorer.statistics.mapper;

import com.github.ontio.explorer.statistics.model.GovernanceInfo;
import com.github.ontio.explorer.statistics.model.IncomeInfo;
import org.springframework.stereotype.Repository;

import java.util.List;


@Repository
public interface GovernanceMapper {

    int removeGovernanceInfos();

    int saveGovernanceInfos(List<GovernanceInfo> infos);

    int getMaxIncomeCycle();

    void saveIncomeInfos(List<IncomeInfo> infos, int cycle);
}
