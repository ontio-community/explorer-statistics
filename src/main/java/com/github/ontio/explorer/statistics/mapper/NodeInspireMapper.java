package com.github.ontio.explorer.statistics.mapper;

import com.github.ontio.explorer.statistics.model.NodeInspire;
import org.springframework.stereotype.Repository;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

@Repository
public interface NodeInspireMapper extends Mapper<NodeInspire> {
    // Self-defined SQL.
    int deleteAll();

    int batchInsert(List<NodeInspire> records);
}