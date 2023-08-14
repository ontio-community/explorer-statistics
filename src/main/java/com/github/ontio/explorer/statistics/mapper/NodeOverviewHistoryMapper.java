package com.github.ontio.explorer.statistics.mapper;

import com.github.ontio.explorer.statistics.model.NodeOverviewHistory;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

@Repository
public interface NodeOverviewHistoryMapper extends Mapper<NodeOverviewHistory> {

    int checkHistoryExist();

    int updateRnkEndTime(@Param("roundEndBlock") long roundEndBlock, @Param("roundEndTime") int roundEndTime);

    List<NodeOverviewHistory> queryRoundByTimeStamp(@Param("time") Integer time);

    List<NodeOverviewHistory> queryRoundByCycle(@Param("cycle") Integer cycle);

    NodeOverviewHistory queryNodeDetailByCycle(@Param("cycle") Integer cycle);

    Integer getCurrentRound();
}