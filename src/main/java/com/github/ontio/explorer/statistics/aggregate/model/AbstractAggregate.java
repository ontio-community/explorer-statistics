package com.github.ontio.explorer.statistics.aggregate.model;

import com.github.ontio.explorer.statistics.aggregate.AggregateContext;

import java.util.Optional;

/**
 * @author LiuQi
 */
public abstract class AbstractAggregate<K extends AggregateKey, T> implements Aggregate<K, T> {

	protected final AggregateContext context;

	private final K key;

	private transient String lastTxHash;

	protected AbstractAggregate(AggregateContext context, K key) {
		this.context = context;
		this.key = key;
	}

	@Override
	public K key() {
		return key;
	}

	@Override
	public void aggregate(TransactionInfo transactionInfo) {
		if (transactionInfo.isTransfer()) {
			aggregateTransfer(transactionInfo);
		} else if (transactionInfo.isGas()) {
			aggregateGas(transactionInfo);
		}
		this.lastTxHash = transactionInfo.getTxHash();
	}

	protected abstract void aggregateTransfer(TransactionInfo transactionInfo);

	protected abstract void aggregateGas(TransactionInfo transactionInfo);

	public void populate(T baseline, T total) {
		populateBaseline(baseline);
		populateTotal(total);
	}

	protected abstract void populateBaseline(T baseline);

	protected abstract void populateTotal(T total);

	@Override
	public Optional<T> snapshot(boolean total) {
		return total ? snapshotTotal() : snapshot();
	}

	protected abstract Optional<T> snapshot();

	protected abstract Optional<T> snapshotTotal();

	@Override
	public void rebase(boolean reSyncing) {
		populateBaseline(null);
		if (reSyncing) {
			populateTotal(null);
		}
	}

	protected boolean isTxHashChanged(TransactionInfo transactionInfo) {
		return !transactionInfo.getTxHash().equals(lastTxHash);
	}

}
