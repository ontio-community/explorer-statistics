package com.github.ontio.explorer.statistics.aggregate.model;

import com.github.ontio.explorer.statistics.model.AddressDailyAggregation;
import com.github.ontio.explorer.statistics.model.ContractDailyAggregation;
import com.github.ontio.explorer.statistics.model.TokenDailyAggregation;
import lombok.Getter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author LiuQi
 */
@Getter
public class TotalAggregationSnapshot implements Serializable {

	private static final int TOTAL_SNAPSHOT_DATE_ID = -1;

	private List<AddressDailyAggregation> addressAggregations = new ArrayList<>(1024);

	private List<TokenDailyAggregation> tokenAggregations = new ArrayList<>(64);

	private List<ContractDailyAggregation> contractAggregations = new ArrayList<>(64);

	public void append(Iterable<Aggregate<?, ?>> aggregates) {
		aggregates.forEach(aggregate -> {
			if (aggregate instanceof AddressAggregate) {
				append((AddressAggregate) aggregate);
			} else if (aggregate instanceof TokenAggregate) {
				append((TokenAggregate) aggregate);
			} else if (aggregate instanceof ContractAggregate) {
				append((ContractAggregate) aggregate);
			}
		});
	}

	private void append(AddressAggregate aggregate) {
		aggregate.snapshot(true).ifPresent(aggregation -> {
			if (aggregate.key().isForCalledContractHash()) {
				aggregation.setDateId(AddressAggregate.CALLED_CONTRACT_HASH_AGGREGATION_DATE_ID);
			} else {
				aggregation.setDateId(TOTAL_SNAPSHOT_DATE_ID);
			}
			addressAggregations.add(aggregation);
		});
	}

	private void append(TokenAggregate aggregate) {
		aggregate.snapshot(true).ifPresent(aggregation -> {
			aggregation.setDateId(TOTAL_SNAPSHOT_DATE_ID);
			tokenAggregations.add(aggregation);
		});
	}

	private void append(ContractAggregate aggregate) {
		aggregate.snapshot(true).ifPresent(aggregation -> {
			aggregation.setDateId(TOTAL_SNAPSHOT_DATE_ID);
			contractAggregations.add(aggregation);
		});
	}

}
