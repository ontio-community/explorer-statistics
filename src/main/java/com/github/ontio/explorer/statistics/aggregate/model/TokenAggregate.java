package com.github.ontio.explorer.statistics.aggregate.model;

import com.github.ontio.explorer.statistics.aggregate.AggregateContext;
import com.github.ontio.explorer.statistics.aggregate.support.UniqueCounter;
import com.github.ontio.explorer.statistics.common.Constants;
import com.github.ontio.explorer.statistics.model.TokenDailyAggregation;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.math.BigDecimal;
import java.util.Optional;

import static java.math.BigDecimal.ZERO;

/**
 * @author LiuQi
 */
public class TokenAggregate extends AbstractAggregate<TokenAggregate.TokenAggregateKey, TokenDailyAggregation> {

	private int txCount;

	private BigDecimal txAmount;

	private UniqueCounter<String> depositAddressCounter;

	private UniqueCounter<String> withdrawAddressCounter;

	private UniqueCounter<String> txAddressCounter;

	private BigDecimal feeAmount;

	private transient boolean changed;

	private TotalAggregate total;

	public TokenAggregate(AggregateContext context, TokenAggregateKey key) {
		super(context, key);
	}

	@Override
	protected void aggregateTransfer(TransactionInfo transactionInfo) {
		BigDecimal amount = transactionInfo.getAmount();
		String from = transactionInfo.getFromAddress();
		String to = transactionInfo.getToAddress();

		if (isTxHashChanged(transactionInfo)) {
			this.txCount++;
			this.total.txCount++;
		}

		this.txAmount = this.txAmount.add(amount);
		this.depositAddressCounter.count(to);
		this.withdrawAddressCounter.count(from);
		this.txAddressCounter.count(from, to);

		this.total.txAmount = this.total.txAmount.add(amount);

		this.changed = true;
		this.total.changed = true;
	}

	@Override
	protected void aggregateGas(TransactionInfo transactionInfo) {
		BigDecimal fee = transactionInfo.getFee();
		BigDecimal amount = transactionInfo.getAmount();
		String from = transactionInfo.getFromAddress();
		String to = transactionInfo.getToAddress();

		if (isTxHashChanged(transactionInfo)) {
			this.txCount++;
			this.total.txCount++;
		}
		if (this.txCount > 0) {
			this.depositAddressCounter.count(to);
			this.withdrawAddressCounter.count(from);
			this.txAddressCounter.count(from, to);
		}

		this.txAmount = this.txAmount.add(amount);
		this.feeAmount = this.feeAmount.add(fee);
		this.total.txAmount = this.total.txAmount.add(amount);
		this.total.feeAmount = this.total.feeAmount.add(fee);

		this.changed = true;
		this.total.changed = true;
	}

	@Override
	protected void populateBaseline(TokenDailyAggregation baseline) {
		this.txCount = 0;
		this.txAmount = BigDecimal.ZERO;
		this.depositAddressCounter = new UniqueCounter.SimpleUniqueCounter<>();
		this.withdrawAddressCounter = new UniqueCounter.SimpleUniqueCounter<>();
		this.txAddressCounter = new UniqueCounter.SimpleUniqueCounter<>();
		this.feeAmount = BigDecimal.ZERO;
		this.changed = false;

		if (this.total != null) {
			this.total.changed = false;
		}
	}

	@Override
	protected void populateTotal(TokenDailyAggregation total) {
		if (this.total == null) {
			this.total = new TotalAggregate();
		}
		if (total != null) {
			this.total.txCount = total.getTxCount();
			this.total.txAmount = total.getTxAmount();
			this.total.feeAmount = total.getFeeAmount();
		}
	}

	@Override
	protected Optional<TokenDailyAggregation> snapshot() {
		if (!changed) {
			return Optional.empty();
		}
		if (Constants.MAX_VALUE.compareTo(txAmount) < 0) {
			txAmount = Constants.MAX_VALUE;
		}
		if (Constants.MIN_VALUE.compareTo(txAmount) > 0) {
			txAmount = Constants.MIN_VALUE;
		}

		TokenDailyAggregation snapshot = new TokenDailyAggregation();
		snapshot.setTokenContractHash(key().getTokenContractHash());
		snapshot.setDateId(context.getDateId());
		snapshot.setUsdPrice(ZERO);
		snapshot.setTxCount(txCount);
		snapshot.setTxAmount(txAmount);
		snapshot.setDepositAddressCount(depositAddressCounter.getCount());
		snapshot.setWithdrawAddressCount(withdrawAddressCounter.getCount());
		snapshot.setTxAddressCount(txAddressCounter.getCount());
		snapshot.setFeeAmount(feeAmount);
		return Optional.of(snapshot);
	}

	@Override
	protected Optional<TokenDailyAggregation> snapshotTotal() {
		if (!total.changed) {
			return Optional.empty();
		}
		if (Constants.MAX_VALUE.compareTo(total.txAmount) < 0) {
			total.txAmount = Constants.MAX_VALUE;
		}
		if (Constants.MIN_VALUE.compareTo(total.txAmount) > 0) {
			total.txAmount = Constants.MIN_VALUE;
		}

		TokenDailyAggregation snapshot = new TokenDailyAggregation();
		snapshot.setTokenContractHash(key().getTokenContractHash());
		snapshot.setDateId(context.getConfig().getTotalAggregationDateId());
		snapshot.setUsdPrice(ZERO);
		snapshot.setTxCount(total.txCount);
		snapshot.setTxAmount(total.txAmount);
		snapshot.setDepositAddressCount(0);
		snapshot.setWithdrawAddressCount(0);
		snapshot.setTxAddressCount(0);
		snapshot.setFeeAmount(total.feeAmount);
		return Optional.of(snapshot);
	}

	@RequiredArgsConstructor
	@EqualsAndHashCode
	@Getter
	@ToString
	public static class TokenAggregateKey implements AggregateKey {
		private final String tokenContractHash;
	}

	private static class TotalAggregate {
		private int txCount;
		private BigDecimal txAmount = ZERO;
		private BigDecimal feeAmount = ZERO;
		private transient boolean changed;
	}

}
