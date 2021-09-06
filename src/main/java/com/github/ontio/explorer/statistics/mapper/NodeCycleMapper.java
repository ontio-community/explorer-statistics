package com.github.ontio.explorer.statistics.mapper;

import com.github.ontio.explorer.statistics.model.NodeCycle;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;
import tk.mybatis.mapper.common.Mapper;

import java.math.BigDecimal;
import java.util.List;

@Repository
public interface NodeCycleMapper extends Mapper<NodeCycle> {

    int batchSave(@Param("nodeCycleList") List<NodeCycle> nodeCycleList);

    int selectMaxCycle();

    List<NodeCycle> selectCycleData(Integer cycle);

    // 根据publicKey,更新库中上次数据的node_proportion_t2和user_proportion_t2
    int updateLastCycleProportion(@Param("publicKey") String publicKey, @Param("cycle") Integer cycle, @Param("nodeProportionT2") String nodeProportionT2, @Param("userProportionT2") String userProportionT2, @Param("bonusOng") BigDecimal bonusOng);


}