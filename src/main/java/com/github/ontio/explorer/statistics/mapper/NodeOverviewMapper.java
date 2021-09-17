package com.github.ontio.explorer.statistics.mapper;

import com.github.ontio.explorer.statistics.model.NodeOverview;
import org.springframework.stereotype.Repository;
import tk.mybatis.mapper.common.Mapper;

@Repository
public interface NodeOverviewMapper extends Mapper<NodeOverview> {

    void updateBlkCntToNxtRnd(Long blkCntToNxtRnd);

    Long selectBlkCountToNxtRnd();

    Long updateLeftTimeToNxtRnd(Long leftTimeToNextRnd);
}