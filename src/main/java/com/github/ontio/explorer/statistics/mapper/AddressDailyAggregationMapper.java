package com.github.ontio.explorer.statistics.mapper;

import com.github.ontio.explorer.statistics.model.AddressDailyAggregation;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

public interface AddressDailyAggregationMapper extends Mapper<AddressDailyAggregation> {

    int batchSave(List<AddressDailyAggregation> aggregations);

    int reSync(AddressDailyAggregation aggregation);

    int reSyncBalance(AddressDailyAggregation aggregation);

    AddressDailyAggregation selectSumTxCount(String address, String contractHash);

    AddressDailyAggregation selectTxCount(String address, String contractHash, int dateId);

    int updateTxCount(AddressDailyAggregation aggregation);

}