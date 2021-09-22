package com.github.ontio.explorer.statistics.mapper;

import com.github.ontio.explorer.statistics.model.NodeInfoOffChain;
import org.springframework.stereotype.Repository;
import tk.mybatis.mapper.common.Mapper;

@Repository
public interface NodeInfoOffChainMapper extends Mapper<NodeInfoOffChain> {
    String selectNameByPublicKey(String publicKey);

    int syncWithOnChainNodes();

    int updateStableNodeByPubKey(String publicKey);

}