package com.github.ontio.explorer.statistics.aggregate.model;

import com.github.ontio.explorer.statistics.aggregate.AggregateContext;
import com.github.ontio.explorer.statistics.aggregate.support.BigDecimalRanker;
import com.github.ontio.explorer.statistics.aggregate.support.UniqueCounter;
import com.github.ontio.explorer.statistics.model.AddressDailyAggregation;
import lombok.AllArgsConstructor;
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
public class AddressAggregate extends AbstractAggregate<AddressAggregate.AddressAggregateKey, AddressDailyAggregation> {

	public static final int CALLED_CONTRACT_HASH_AGGREGATION_DATE_ID = Integer.MIN_VALUE;

	private BigDecimal previousBalance;

	private BigDecimal balance;

	private int depositTxCount;

	private int withdrawTxCount;

	private BigDecimal depositAmount;

	private BigDecimal withdrawAmount;

	private BigDecimal feeAmount;

	private BigDecimalRanker balanceRanker;

	private UniqueCounter<String> depositAddressCounter;

	private UniqueCounter<String> withdrawAddressCounter;

	private UniqueCounter<String> txAddressCounter;

	private UniqueCounter<String> contractCounter;

	private transient boolean changed;

	private TotalAggregate total;

	public AddressAggregate(AggregateContext context, AddressAggregateKey key) {
		super(context, key);
	}

	@Override
	protected void aggregateTransfer(TransactionInfo transactionInfo) {
		String from = transactionInfo.getFromAddress();
		String to = transactionInfo.getToAddress();

		contractCounter.count(transactionInfo.getContractHash());
		if (context.virtualContracts().contains(key().getTokenContractHash()) || key().isForCalledContractHash()) {
			if (from.equals(key().getAddress())) {
				if (isTxHashChanged(transactionInfo)) {
					withdrawTxCount++;
					total.withdrawTxCount++;
				}
				withdrawAddressCounter.count(to);
				txAddressCounter.count(to);
				if (transactionInfo.isSelfTransaction()) {
					depositAddressCounter.count(from);
				}
			} else if (to.equals(key().getAddress())) {
				if (isTxHashChanged(transactionInfo)) {
					depositTxCount++;
					total.depositTxCount++;
				}
				depositAddressCounter.count(from);
				txAddressCounter.count(from);
			}
		} else {
			BigDecimal amount = transactionInfo.getAmount();
			if (from.equals(key().getAddress())) {
				if (isTxHashChanged(transactionInfo)) {
					withdrawTxCount++;
					total.withdrawTxCount++;
				}

				withdrawAmount = withdrawAmount.add(amount);
				withdrawAddressCounter.count(to);
				txAddressCounter.count(to);
				total.withdrawAmount = total.withdrawAmount.add(amount);

				if (transactionInfo.isSelfTransaction()) {
					depositAmount = depositAmount.add(amount);
					depositAddressCounter.count(from);
					total.depositAmount = total.depositAmount.add(amount);
				} else {
					balance = balance.subtract(amount);
					total.balance = total.balance.subtract(amount);
				}
			} else if (to.equals(key().getAddress())) {
				if (isTxHashChanged(transactionInfo)) {
					depositTxCount++;
					total.depositTxCount++;
				}

				balance = balance.add(amount);
				depositAmount = depositAmount.add(amount);
				depositAddressCounter.count(from);
				txAddressCounter.count(from);

				total.balance = total.balance.add(amount);
				total.depositAmount = total.depositAmount.add(amount);
			}
			balanceRanker.rank(balance);
			total.balanceRanker.rank(total.balance);
		}
		changed = true;
		total.changed = true;
	}

	@Override
	protected void aggregateGas(TransactionInfo transactionInfo) {
		BigDecimal amount = transactionInfo.getAmount();
		String from = transactionInfo.getFromAddress();
		String to = transactionInfo.getToAddress();

		contractCounter.count(transactionInfo.getContractHash());
		if (context.virtualContracts().contains(key().getTokenContractHash()) || key().isForCalledContractHash()) {
			if (from.equals(key().getAddress())) {
				if (isTxHashChanged(transactionInfo)) {
					withdrawTxCount++;
					total.withdrawTxCount++;
				}
				withdrawAddressCounter.count(to);
				txAddressCounter.count(to);
				if (transactionInfo.isSelfTransaction()) {
					depositAddressCounter.count(from);
				}
				feeAmount = feeAmount.add(transactionInfo.getFee());
				total.feeAmount = total.feeAmount.add(transactionInfo.getFee());
			} else if (to.equals(key().getAddress())) {
				if (isTxHashChanged(transactionInfo)) {
					depositTxCount++;
					total.depositTxCount++;
				}
				depositAddressCounter.count(from);
				txAddressCounter.count(from);
			}
		} else {
			if (from.equals(key().getAddress())) {
				if (isTxHashChanged(transactionInfo)) {
					withdrawTxCount++;
					total.withdrawTxCount++;
				}

				withdrawAmount = withdrawAmount.add(amount);
				withdrawAddressCounter.count(to);
				txAddressCounter.count(to);
				total.withdrawAmount = total.withdrawAmount.add(amount);

				if (transactionInfo.isSelfTransaction()) {
					depositAmount = depositAmount.add(amount);
					depositAddressCounter.count(from);
					total.depositAmount = total.depositAmount.add(amount);
				} else {
					balance = balance.subtract(amount);
					total.balance = total.balance.subtract(amount);
				}
			} else if (to.equals(key().getAddress())) {
				if (isTxHashChanged(transactionInfo)) {
					depositTxCount++;
					total.depositTxCount++;
				}
				balance = balance.add(amount);
				depositAmount = depositAmount.add(amount);
				depositAddressCounter.count(from);
				txAddressCounter.count(from);
				total.balance = total.balance.add(amount);
				total.depositAmount = total.depositAmount.add(amount);
			}
			balanceRanker.rank(balance);
			total.balanceRanker.rank(total.balance);
		}
		changed = true;
		total.changed = true;
	}

	@Override
	protected void populateBaseline(AddressDailyAggregation baseline) {
		if (baseline != null) {
			this.balance = baseline.getBalance();
		}
		if (this.balance == null) {
			this.balance = ZERO;
		}
		this.previousBalance = this.balance;
		this.depositTxCount = 0;
		this.withdrawTxCount = 0;
		this.depositAmount = ZERO;
		this.withdrawAmount = ZERO;
		this.feeAmount = ZERO;
		this.balanceRanker = new BigDecimalRanker(this.balance);
		this.depositAddressCounter = new UniqueCounter.SimpleUniqueCounter<>();
		this.withdrawAddressCounter = new UniqueCounter.SimpleUniqueCounter<>();
		this.txAddressCounter = new UniqueCounter.SimpleUniqueCounter<>();
		this.feeAmount = ZERO;
		this.contractCounter = new UniqueCounter.SimpleUniqueCounter<>();
		this.changed = false;

		if (this.total != null) {
			this.total.changed = false;
		}
	}

	@Override
	protected void populateTotal(AddressDailyAggregation total) {
		if (this.total == null) {
			this.total = new TotalAggregate();
		}
		if (total != null) {
			this.total.balance = total.getBalance();
			this.total.depositTxCount = total.getDepositTxCount();
			this.total.withdrawTxCount = total.getWithdrawTxCount();
			this.total.depositAmount = total.getDepositAmount();
			this.total.withdrawAmount = total.getWithdrawAmount();
			this.total.feeAmount = total.getFeeAmount();
			this.total.balanceRanker.rank(total.getMaxBalance());
			this.total.balanceRanker.rank(total.getMinBalance());
		}
	}

	@Override
	protected Optional<AddressDailyAggregation> snapshot() {
		if (!changed || key().isForCalledContractHash()) {
			return Optional.empty();
		}

		AddressDailyAggregation snapshot = new AddressDailyAggregation();
		snapshot.setAddress(key().getAddress());
		snapshot.setTokenContractHash(key().getTokenContractHash());
		snapshot.setDateId(context.getDateId());
		snapshot.setBalance(balance);
		snapshot.setUsdPrice(ZERO); // TODO 0 for now
		snapshot.setMaxBalance(balanceRanker.getMax());
		snapshot.setMinBalance(balanceRanker.getMin());
		snapshot.setDepositTxCount(depositTxCount);
		snapshot.setWithdrawTxCount(withdrawTxCount);
		snapshot.setDepositAmount(depositAmount);
		snapshot.setWithdrawAmount(withdrawAmount);
		snapshot.setDepositAddressCount(depositAddressCounter.getCount());
		snapshot.setWithdrawAddressCount(withdrawAddressCounter.getCount());
		snapshot.setTxAddressCount(txAddressCounter.getCount());
		snapshot.setFeeAmount(feeAmount);
		snapshot.setContractCount(contractCounter.getCount());
		snapshot.setIsVirtual(context.virtualContracts().contains(key().getTokenContractHash()));
		snapshot.setPreviousBalance(previousBalance);
		return Optional.of(snapshot);
	}

	@Override
	protected Optional<AddressDailyAggregation> snapshotTotal() {
		if (!total.changed) {
			return Optional.empty();
		}

		AddressDailyAggregation snapshot = new AddressDailyAggregation();
		snapshot.setAddress(key().getAddress());
		snapshot.setTokenContractHash(key().getTokenContractHash());
		if (key().isForCalledContractHash()) {
			snapshot.setDateId(CALLED_CONTRACT_HASH_AGGREGATION_DATE_ID);
		} else {
			snapshot.setDateId(context.getConfig().getTotalAggregationDateId());
		}
		snapshot.setBalance(total.balance);
		snapshot.setUsdPrice(ZERO); // TODO 0 for now
		snapshot.setMaxBalance(total.balanceRanker.getMax());
		snapshot.setMinBalance(total.balanceRanker.getMin());
		snapshot.setDepositTxCount(total.depositTxCount);
		snapshot.setWithdrawTxCount(total.withdrawTxCount);
		snapshot.setDepositAmount(total.depositAmount);
		snapshot.setWithdrawAmount(total.withdrawAmount);
		snapshot.setDepositAddressCount(0);
		snapshot.setWithdrawAddressCount(0);
		snapshot.setTxAddressCount(0);
		snapshot.setFeeAmount(total.feeAmount);
		snapshot.setContractCount(0);
		snapshot.setIsVirtual(context.virtualContracts().contains(key().getTokenContractHash()));
		snapshot.setPreviousBalance(previousBalance);
		return Optional.of(snapshot);
	}

	@RequiredArgsConstructor
	@AllArgsConstructor
	@EqualsAndHashCode
	@Getter
	@ToString
	public static class AddressAggregateKey implements AggregateKey {
		private final String address;
		private final String tokenContractHash;
		private boolean forCalledContractHash;
	}

	private static class TotalAggregate {
		private BigDecimal balance = ZERO;
		private int depositTxCount;
		private int withdrawTxCount;
		private BigDecimal depositAmount = ZERO;
		private BigDecimal withdrawAmount = ZERO;
		private BigDecimal feeAmount = ZERO;
		private BigDecimalRanker balanceRanker = new BigDecimalRanker(ZERO);
		private transient boolean changed;
	}

}
