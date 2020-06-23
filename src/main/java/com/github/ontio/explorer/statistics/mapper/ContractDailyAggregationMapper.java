package com.github.ontio.explorer.statistics.mapper;

import com.github.ontio.explorer.statistics.model.ContractDailyAggregation;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

public interface ContractDailyAggregationMapper extends Mapper<ContractDailyAggregation> {

	int batchSave(List<ContractDailyAggregation> aggregations);

	int batchReSync(List<ContractDailyAggregation> aggregations);

}