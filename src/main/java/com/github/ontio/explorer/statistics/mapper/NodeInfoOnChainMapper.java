package com.github.ontio.explorer.statistics.mapper;

import com.github.ontio.explorer.statistics.model.NodeInfoOnChain;
import org.springframework.stereotype.Repository;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

@Repository
public interface NodeInfoOnChainMapper extends Mapper<NodeInfoOnChain> {
    NodeInfoOnChain selectByPublicKey(String publicKey);

    int deleteAll();

    int batchInsert(List<NodeInfoOnChain> records);
}