package com.github.ontio.explorer.statistics.aggregate.model;

import com.github.ontio.OntSdk;
import com.github.ontio.explorer.statistics.aggregate.AggregateContext;
import com.github.ontio.explorer.statistics.aggregate.support.BigDecimalRanker;
import com.github.ontio.explorer.statistics.aggregate.support.UniqueCounter;
import com.github.ontio.explorer.statistics.common.Constants;
import com.github.ontio.explorer.statistics.model.AddressDailyAggregation;
import com.github.ontio.explorer.statistics.util.HttpUtil;
import com.github.ontio.sdk.exception.SDKException;
import lombok.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Optional;

import static java.math.BigDecimal.ZERO;

/**
 * @author LiuQi
 */
public class AddressAggregate extends AbstractAggregate<AddressAggregate.AddressAggregateKey, AddressDailyAggregation> {

    public static final int OEP_AGGREGATION_DATE_ID = Integer.MIN_VALUE;

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
        int blockHeight = transactionInfo.getBlockHeight();

        contractCounter.count(transactionInfo.getContractHash());
        if (context.virtualContracts().contains(key().getTokenContractHash()) || key().isForOep()) {
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
                    // ONG两个地址互通，balance需要实时查询
                    if (Constants.ONG_CONTRACT_HASH.equals(key().getTokenContractHash()) && blockHeight >= context.getConfig().getEvmActiveBlock()) {
                        BigDecimal ongBalance = getOngBalance(key().getAddress());
                        if (null == ongBalance) {
                            balance = balance.subtract(amount);
                            total.balance = total.balance.subtract(amount);
                        } else {
                            balance = ongBalance;
                            total.balance = ongBalance;
                        }
                    } else {
                        balance = balance.subtract(amount);
                        total.balance = total.balance.subtract(amount);
                    }
                }
            } else if (to.equals(key().getAddress())) {
                if (isTxHashChanged(transactionInfo)) {
                    depositTxCount++;
                    total.depositTxCount++;
                }

                if (Constants.ONG_CONTRACT_HASH.equals(key().getTokenContractHash()) && blockHeight >= context.getConfig().getEvmActiveBlock()) {
                    BigDecimal ongBalance = getOngBalance(key().getAddress());
                    if (null == ongBalance) {
                        balance = balance.add(amount);
                        total.balance = total.balance.add(amount);
                    } else {
                        balance = ongBalance;
                        total.balance = ongBalance;
                    }
                } else {
                    balance = balance.add(amount);
                    total.balance = total.balance.add(amount);
                }

                depositAmount = depositAmount.add(amount);
                depositAddressCounter.count(from);
                txAddressCounter.count(from);
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
        int blockHeight = transactionInfo.getBlockHeight();

        contractCounter.count(transactionInfo.getContractHash());
        if (context.virtualContracts().contains(key().getTokenContractHash()) || key().isForOep()) {
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
                    if (Constants.ONG_CONTRACT_HASH.equals(key().getTokenContractHash()) && blockHeight >= context.getConfig().getEvmActiveBlock()) {
                        BigDecimal ongBalance = getOngBalance(key().getAddress());
                        if (null == ongBalance) {
                            balance = balance.subtract(amount);
                            total.balance = total.balance.subtract(amount);
                        } else {
                            balance = ongBalance;
                            total.balance = ongBalance;
                        }
                    } else {
                        balance = balance.subtract(amount);
                        total.balance = total.balance.subtract(amount);
                    }
                }
            } else if (to.equals(key().getAddress())) {
                if (isTxHashChanged(transactionInfo)) {
                    depositTxCount++;
                    total.depositTxCount++;
                }

                if (Constants.ONG_CONTRACT_HASH.equals(key().getTokenContractHash()) && blockHeight >= context.getConfig().getEvmActiveBlock()) {
                    BigDecimal ongBalance = getOngBalance(key().getAddress());
                    if (null == ongBalance) {
                        balance = balance.add(amount);
                        total.balance = total.balance.add(amount);
                    } else {
                        balance = ongBalance;
                        total.balance = ongBalance;
                    }
                } else {
                    balance = balance.add(amount);
                    total.balance = total.balance.add(amount);
                }

                depositAmount = depositAmount.add(amount);
                depositAddressCounter.count(from);
                txAddressCounter.count(from);
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
        if (!changed || key().isForOep()) {
            return Optional.empty();
        }
        if (Constants.MAX_VALUE.compareTo(balance) < 0) {
            balance = Constants.MAX_VALUE;
        }
        if (Constants.MIN_VALUE.compareTo(balance) > 0) {
            balance = Constants.MIN_VALUE;
        }
        BigDecimal maxBalance = balanceRanker.getMax();
        BigDecimal minBalance = balanceRanker.getMin();
        if (Constants.MAX_VALUE.compareTo(maxBalance) < 0) {
            maxBalance = Constants.MAX_VALUE;
        }
        if (Constants.MIN_VALUE.compareTo(maxBalance) > 0) {
            maxBalance = Constants.MIN_VALUE;
        }
        if (Constants.MAX_VALUE.compareTo(minBalance) < 0) {
            minBalance = Constants.MAX_VALUE;
        }
        if (Constants.MIN_VALUE.compareTo(minBalance) > 0) {
            minBalance = Constants.MIN_VALUE;
        }
        if (Constants.MAX_VALUE.compareTo(depositAmount) < 0) {
            depositAmount = Constants.MAX_VALUE;
        }
        if (Constants.MIN_VALUE.compareTo(depositAmount) > 0) {
            depositAmount = Constants.MIN_VALUE;
        }
        if (Constants.MAX_VALUE.compareTo(withdrawAmount) < 0) {
            withdrawAmount = Constants.MAX_VALUE;
        }
        if (Constants.MIN_VALUE.compareTo(withdrawAmount) > 0) {
            withdrawAmount = Constants.MIN_VALUE;
        }

        AddressDailyAggregation snapshot = new AddressDailyAggregation();
        snapshot.setAddress(key().getAddress());
        snapshot.setTokenContractHash(key().getTokenContractHash());
        snapshot.setDateId(context.getDateId());
        snapshot.setBalance(balance);
        snapshot.setUsdPrice(ZERO);
        snapshot.setMaxBalance(maxBalance);
        snapshot.setMinBalance(minBalance);
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
        if (Constants.MAX_VALUE.compareTo(total.balance) < 0) {
            total.balance = Constants.MAX_VALUE;
        }
        if (Constants.MIN_VALUE.compareTo(total.balance) > 0) {
            total.balance = Constants.MIN_VALUE;
        }
        BigDecimal maxBalance = total.balanceRanker.getMax();
        BigDecimal minBalance = total.balanceRanker.getMin();
        if (Constants.MAX_VALUE.compareTo(maxBalance) < 0) {
            maxBalance = Constants.MAX_VALUE;
        }
        if (Constants.MIN_VALUE.compareTo(maxBalance) > 0) {
            maxBalance = Constants.MIN_VALUE;
        }
        if (Constants.MAX_VALUE.compareTo(minBalance) < 0) {
            minBalance = Constants.MAX_VALUE;
        }
        if (Constants.MIN_VALUE.compareTo(minBalance) > 0) {
            minBalance = Constants.MIN_VALUE;
        }
        if (Constants.MAX_VALUE.compareTo(total.depositAmount) < 0) {
            total.depositAmount = Constants.MAX_VALUE;
        }
        if (Constants.MIN_VALUE.compareTo(total.depositAmount) > 0) {
            total.depositAmount = Constants.MIN_VALUE;
        }
        if (Constants.MAX_VALUE.compareTo(total.withdrawAmount) < 0) {
            total.withdrawAmount = Constants.MAX_VALUE;
        }
        if (Constants.MIN_VALUE.compareTo(total.withdrawAmount) > 0) {
            total.withdrawAmount = Constants.MIN_VALUE;
        }

        AddressDailyAggregation snapshot = new AddressDailyAggregation();
        snapshot.setAddress(key().getAddress());
        snapshot.setTokenContractHash(key().getTokenContractHash());
        if (key().isForOep()) {
            snapshot.setDateId(OEP_AGGREGATION_DATE_ID);
        } else {
            snapshot.setDateId(context.getConfig().getTotalAggregationDateId());
        }
        snapshot.setBalance(total.balance);
        snapshot.setUsdPrice(ZERO);
        snapshot.setMaxBalance(maxBalance);
        snapshot.setMinBalance(minBalance);
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
        private boolean forOep;
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

    public BigDecimal getOngBalance(String address) {
        OntSdk sdk = OntSdk.getInstance();
        try {
            sdk.getRestful();
        } catch (SDKException e) {
            sdk.setRestful(context.getConfig().getHosts().get(0));
        }
        try {
            if (address.startsWith(Constants.EVM_PREFIX)) {
                address = HttpUtil.ethAddrToOntAddr(address);
            }
            BigInteger amount = sdk.nativevm().ongV2().queryBalanceOf(address);
            return new BigDecimal(amount).divide(Constants.ONG_DECIMAL, 18, RoundingMode.DOWN).stripTrailingZeros();
        } catch (Exception e) {
            return null;
        }
    }
}
