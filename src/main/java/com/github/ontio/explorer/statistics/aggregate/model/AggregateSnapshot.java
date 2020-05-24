package com.github.ontio.explorer.statistics.aggregate.model;

import com.github.ontio.explorer.statistics.model.AddressDailyAggregation;
import com.github.ontio.explorer.statistics.model.ContractDailyAggregation;
import com.github.ontio.explorer.statistics.model.TokenDailyAggregation;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author LiuQi
 */
@Getter
@RequiredArgsConstructor
public class AggregateSnapshot implements Serializable {

	private final int currentDateId;

	private final int lastBlockHeight;

	private final ReSync reSync;

	private Set<AggregateKey> aggregateKeys = new HashSet<>();

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
		aggregateKeys.add(aggregate.key());
		aggregate.snapshot(false).ifPresent(addressAggregations::add);
		aggregate.snapshot(true).ifPresent(addressAggregations::add);
	}

	private void append(TokenAggregate aggregate) {
		aggregateKeys.add(aggregate.key());
		aggregate.snapshot(false).ifPresent(tokenAggregations::add);
		aggregate.snapshot(true).ifPresent(tokenAggregations::add);
	}

	private void append(ContractAggregate aggregate) {
		aggregateKeys.add(aggregate.key());
		aggregate.snapshot(false).ifPresent(contractAggregations::add);
		aggregate.snapshot(true).ifPresent(contractAggregations::add);
	}

}
