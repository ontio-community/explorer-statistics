package com.github.ontio.explorer.statistics.aggregate.model;

import java.io.Serializable;
import java.util.Optional;

/**
 * @author LiuQi
 */
public interface Aggregate<K extends AggregateKey, T> extends Serializable {

	K key();

	void aggregate(TransactionInfo transactionInfo);

	void populate(T baseline, T total);

	Optional<T> snapshot(boolean total);

	void rebase(boolean reSyncing);

}
