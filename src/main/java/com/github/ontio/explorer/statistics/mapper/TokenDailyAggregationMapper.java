package com.github.ontio.explorer.statistics.mapper;

import com.github.ontio.explorer.statistics.model.TokenDailyAggregation;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

public interface TokenDailyAggregationMapper extends Mapper<TokenDailyAggregation> {

	int batchSave(List<TokenDailyAggregation> aggregations);

	int batchReSync(List<TokenDailyAggregation> aggregations);

}