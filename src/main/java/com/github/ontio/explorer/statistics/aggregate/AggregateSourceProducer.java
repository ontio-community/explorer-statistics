package com.github.ontio.explorer.statistics.aggregate;

import com.github.ontio.explorer.statistics.aggregate.model.ReSync;
import com.github.ontio.explorer.statistics.aggregate.model.Tick;
import com.github.ontio.explorer.statistics.aggregate.model.TransactionInfo;
import com.github.ontio.explorer.statistics.aggregate.support.DisruptorEventDispatcher;
import com.github.ontio.explorer.statistics.common.ParamsConfig;
import com.github.ontio.explorer.statistics.mapper.AddressDailyAggregationMapper;
import com.github.ontio.explorer.statistics.mapper.ContractMapper;
import com.github.ontio.explorer.statistics.mapper.CurrentMapper;
import com.github.ontio.explorer.statistics.mapper.TxDetailMapper;
import com.github.ontio.explorer.statistics.model.AddressDailyAggregation;
import com.github.ontio.explorer.statistics.model.Contract;
import com.github.ontio.explorer.statistics.model.TxDetail;
import com.google.common.util.concurrent.RateLimiter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.RowBounds;
import org.springframework.beans.BeanUtils;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.PostConstruct;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author LiuQi
 */
@RequiredArgsConstructor
@Component
@Slf4j
public class AggregateSourceProducer {

    private static final int BATCH_SIZE = 5000;

    private static final int BLOCK_BATCH_SIZE = 1000;

    private final DisruptorEventDispatcher dispatcher;

    private final TxDetailMapper txDetailMapper;

    private final AddressDailyAggregationMapper addressDailyAggregationMapper;

    private final CurrentMapper currentMapper;

    private final ContractMapper contractMapper;

    private final ParamsConfig config;

    private AtomicBoolean reSyncing;

    private RateLimiter rateLimiter;

    private volatile int startTxDetailId;

    private volatile int startBlockHeight;

    public void produceTransactionInfo() {
        int id = startTxDetailId;

        try {
            while (true) {
                Example example = new Example(TxDetail.class);
                example.and().andGreaterThan("id", id).andIn("eventType", Arrays.asList(2, 3));
                example.orderBy("id");
                List<TxDetail> details = txDetailMapper.selectByExampleAndRowBounds(example, new RowBounds(0, BATCH_SIZE));

                if (details == null || details.isEmpty()) {
                    break;
                }

                for (TxDetail detail : details) {
                    if (rateLimiter != null) {
                        rateLimiter.acquire();
                    }
                    TransactionInfo transactionInfo = TransactionInfo.wrap(detail);
                    dispatcher.dispatch(transactionInfo);
                    id = detail.getId();
                }
            }
        } finally {
            this.startTxDetailId = id;
        }

    }

    @Scheduled(initialDelay = 5000, fixedDelay = 5000)
    public void productTransactionInfoByBlockHeight() {
        if (reSyncing.get()) {
            // eagerly suspend the publishing if re-sync is enabled
            return;
        }
        Integer latestBlockHeight = txDetailMapper.findLatestBlockHeight();
        if (latestBlockHeight == null) {
            return;
        }
        int blockHeight = this.startBlockHeight;
        int currentBlockHeight = blockHeight;
        try {
            while (blockHeight < latestBlockHeight) {
                Example example = new Example(TxDetail.class);
                example.and()
                        .andGreaterThan("blockHeight", blockHeight)
                        .andLessThanOrEqualTo("blockHeight", blockHeight + BLOCK_BATCH_SIZE)
                        .andIn("eventType", Arrays.asList(2, 3));
                example.orderBy("blockHeight").orderBy("blockIndex").orderBy("txIndex");
                List<TxDetail> details = txDetailMapper.selectByExample(example);

                if (details == null || details.isEmpty()) {
                    blockHeight += BLOCK_BATCH_SIZE;
                    continue;
                }

                for (TxDetail detail : details) {
                    if (reSyncing.get()) {
                        // eagerly suspend the publishing if re-sync is enabled
                        return;
                    }
                    if (rateLimiter != null) {
                        rateLimiter.acquire();
                    }
                    TransactionInfo transactionInfo = TransactionInfo.wrap(detail);
                    dispatcher.dispatch(transactionInfo);
                    blockHeight = detail.getBlockHeight();
                    currentBlockHeight = detail.getBlockHeight();
                }
            }
        } finally {
            this.startBlockHeight = currentBlockHeight;
        }
    }

    @Scheduled(initialDelay = 5000, fixedDelay = 5000)
    public void flushTotalAggregations() {
        if (!reSyncing.get()) {
            dispatcher.dispatch(new Tick(Duration.ofSeconds(5)));
        }
    }

    @PostConstruct
    public void initialize() {
        startBlockHeight = currentMapper.findLastStatBlockHeight();
        log.info("AggregateSourceProducer initialize startBlockHeight:{}", startBlockHeight);
        Integer id = txDetailMapper.findLastIdBeforeBlockHeight(startBlockHeight + 1);
        log.info("AggregateSourceProducer initialize id:{}", id);
        if (id != null) {
            this.startTxDetailId = id;
        }
        if (config.getAggregationRateLimit() > 0) {
            this.rateLimiter = RateLimiter.create(config.getAggregationRateLimit());
        }
        reSyncing = new AtomicBoolean();
    }

    @Scheduled(initialDelay = 1000 * 5, fixedDelay = 1000 * 60 * 10)
    public void triggerReSync() {
        if (config.isReSyncEnabled()) {
            Example example = new Example(Contract.class);
            example.and()
                    .andEqualTo("auditFlag", 1)
                    .andEqualTo("reSyncStatus", 2);
            List<Contract> contracts = contractMapper.selectByExample(example);

            if (contracts != null && !contracts.isEmpty()) {
                try {
                    reSyncing.set(true);
                    TimeUnit.SECONDS.sleep(1);
                    contracts.forEach(this::reSync);
                } catch (InterruptedException e) {
                    log.error("re-sync process interrupted");
                } finally {
                    reSyncing.set(false);
                }
            }
        }
    }

    private void reSync(Contract contract) {
        String contractHash = contract.getContractHash();
        int fromBlock = contract.getReSyncFromBlock();
        int toBlock = contract.getReSyncToBlock();
        if (fromBlock == 0 && toBlock == 0) {
            log.info("contract {} has been fully synchronized", contractHash);
            Contract toUpdate = new Contract();
            toUpdate.setContractHash(contractHash);
            toUpdate.setReSyncStatus(3);
            contractMapper.updateByPrimaryKeySelective(toUpdate);
            return;
        }

        ReSync reSync = new ReSync(contractHash, fromBlock, toBlock);
        dispatcher.dispatch(reSync.begin());

        try {
            if (!reSync.waitToBegin(10, TimeUnit.MINUTES)) {
                log.error("begin re-sync of {} timeout", contractHash);
                return;
            }
        } catch (InterruptedException e) {
            log.error("begin re-sync of {} interrupted", contractHash);
            return;
        }

        // reset the start block height
        startBlockHeight = currentMapper.findLastStatBlockHeight();
        log.info("reSync startBlockHeight:{}", startBlockHeight);

        int beginBlockHeight = Math.max(fromBlock - 1, contract.getReSyncStatBlock());
        int endBlockHeight = Math.min(toBlock, startBlockHeight);

        if (beginBlockHeight < endBlockHeight) {
            log.info("start re-sync of contract {} from {} to {}", contractHash, beginBlockHeight, endBlockHeight);
            while (beginBlockHeight < endBlockHeight) {
                Example example = new Example(TxDetail.class);
                example.and()
                        .andGreaterThan("blockHeight", beginBlockHeight)
                        .andLessThanOrEqualTo("blockHeight", Math.min(beginBlockHeight + BLOCK_BATCH_SIZE, endBlockHeight))
                        .andEqualTo("eventType", 3)
                        .andEqualTo("calledContractHash", contractHash);
                example.orderBy("blockHeight").orderBy("blockIndex").orderBy("txIndex");
                List<TxDetail> details = txDetailMapper.selectByExample(example);

                if (details == null || details.isEmpty()) {
                    log.info("re-sync of contract {} from {} to {} transaction 0", contractHash, beginBlockHeight, Math.min(beginBlockHeight + BLOCK_BATCH_SIZE, endBlockHeight));
                    beginBlockHeight += BLOCK_BATCH_SIZE;
                    continue;
                }

                log.info("re-sync of contract {} from {} to {} transaction {}", contractHash, beginBlockHeight, Math.min(beginBlockHeight + BLOCK_BATCH_SIZE, endBlockHeight), details.size());

                for (TxDetail detail : details) {
                    if (rateLimiter != null) {
                        rateLimiter.acquire();
                    }
                    TransactionInfo transactionInfo = TransactionInfo.wrap(detail);
                    dispatcher.dispatch(transactionInfo);
                    beginBlockHeight = detail.getBlockHeight();
                }
            }
        } else {
            log.info("all transactions of {} haven't been aggregated, no need to re-sync", contractHash);
        }

        dispatcher.dispatch(reSync.end());

        try {
            if (!reSync.waitToEnd(10, TimeUnit.MINUTES)) {
                log.error("end re-sync of {} timeout", contractHash);
            }
        } catch (InterruptedException e) {
            log.error("end re-sync of {} interrupted", contractHash);
        }
        log.info("reSync contract finish {}", contractHash);
    }

    @Scheduled(initialDelay = 1000 * 5, fixedDelay = 1000 * 60 * 10)
    public void triggerFixReSync() {
        if (config.isReSyncEnabled()) {
            // reSyncStatus=4,需要修复重新同步的数据(txCount)
            Example example = new Example(Contract.class);
            example.and()
                    .andEqualTo("auditFlag", 1)
                    .andEqualTo("reSyncStatus", 4);
            List<Contract> contracts = contractMapper.selectByExample(example);

            if (contracts != null && !contracts.isEmpty()) {
                try {
                    reSyncing.set(true);
                    TimeUnit.SECONDS.sleep(1);
                    contracts.forEach(this::fixReSync);
                } catch (InterruptedException e) {
                    log.error("re-sync process interrupted");
                } finally {
                    reSyncing.set(false);
                }
            }
        }
    }

    private void fixReSync(Contract contract) {
        String contractHash = contract.getContractHash();

        Example example = new Example(TxDetail.class);
        example.createCriteria()
                .andEqualTo("eventType", 3)
                .andEqualTo("calledContractHash", contractHash);
        example.selectProperties("fromAddress");
        example.setDistinct(true);
        List<TxDetail> txDetailsFrom = txDetailMapper.selectByExample(example);

        example.selectProperties("toAddress");
        List<TxDetail> txDetailsTo = txDetailMapper.selectByExample(example);

        for (TxDetail txDetail : txDetailsFrom) {
            String fromAddress = txDetail.getFromAddress();
            if (StringUtils.hasLength(fromAddress)) {
                fixAddressTxCount(fromAddress, contractHash, true);
                fixAddressTxCount(fromAddress, AggregateContext.VIRTUAL_CONTRACT_ALL, false);
            }
        }

        for (TxDetail txDetail : txDetailsTo) {
            String toAddress = txDetail.getToAddress();
            if (StringUtils.hasLength(toAddress)) {
                fixAddressTxCount(toAddress, contractHash, true);
                fixAddressTxCount(toAddress, AggregateContext.VIRTUAL_CONTRACT_ALL, false);
            }
        }
        contract.setReSyncStatus(3);
        contractMapper.updateByPrimaryKeySelective(contract);
        log.info("fixReSync contract finish {}", contractHash);
    }

    private void fixAddressTxCount(String address, String contractHash, boolean isOep) {
        // -1:实时,0:每天快照,先修复每天快照0,再用差值更新实时数据-1,-2147483648和-1的值一样
        AddressDailyAggregation addressDailyAggregationSum = addressDailyAggregationMapper.selectSumTxCount(address, contractHash);
        Integer sumDepositTxCount = addressDailyAggregationSum.getDepositTxCount();
        Integer sumWithdrawTxCount = addressDailyAggregationSum.getWithdrawTxCount();
        BigDecimal sumDepositAmount = addressDailyAggregationSum.getDepositAmount();
        BigDecimal sumWithdrawAmount = addressDailyAggregationSum.getWithdrawAmount();

        AddressDailyAggregation addressDailyAggregationSnapShot = addressDailyAggregationMapper.selectTxCount(address, contractHash, 0);
        Integer depositTxCountSnapshot = addressDailyAggregationSnapShot.getDepositTxCount();
        Integer withdrawTxCountSnapshot = addressDailyAggregationSnapShot.getWithdrawTxCount();
        BigDecimal depositAmountSnapshot = addressDailyAggregationSnapShot.getDepositAmount();
        BigDecimal withdrawAmountSnapshot = addressDailyAggregationSnapShot.getWithdrawAmount();
        int depositTxCountGap = depositTxCountSnapshot - sumDepositTxCount;
        int withdrawTxCountGap = withdrawTxCountSnapshot - sumWithdrawTxCount;
        BigDecimal depositAmountGap = depositAmountSnapshot.subtract(sumDepositAmount);
        BigDecimal withdrawAmountGap = withdrawAmountSnapshot.subtract(sumWithdrawAmount);
        addressDailyAggregationSnapShot.setDepositTxCount(sumDepositTxCount);
        addressDailyAggregationSnapShot.setWithdrawTxCount(sumWithdrawTxCount);
        addressDailyAggregationSnapShot.setDepositAmount(sumDepositAmount);
        addressDailyAggregationSnapShot.setWithdrawAmount(sumWithdrawAmount);
        addressDailyAggregationMapper.updateTxCount(addressDailyAggregationSnapShot);

        AddressDailyAggregation addressDailyAggregationRealTime = addressDailyAggregationMapper.selectTxCount(address, contractHash, -1);
        if (addressDailyAggregationRealTime == null) {
            addressDailyAggregationRealTime = new AddressDailyAggregation();
            BeanUtils.copyProperties(addressDailyAggregationSnapShot, addressDailyAggregationRealTime);
            addressDailyAggregationRealTime.setDateId(-1);
            addressDailyAggregationMapper.insertSelective(addressDailyAggregationRealTime);
        } else {
            Integer depositTxCountRealTime = addressDailyAggregationRealTime.getDepositTxCount();
            Integer withdrawTxCountRealTime = addressDailyAggregationRealTime.getWithdrawTxCount();
            BigDecimal depositAmountRealTime = addressDailyAggregationSnapShot.getDepositAmount();
            BigDecimal withdrawAmountRealTime = addressDailyAggregationSnapShot.getWithdrawAmount();
            if (depositTxCountRealTime < depositTxCountSnapshot) {
                addressDailyAggregationRealTime.setDepositTxCount(addressDailyAggregationSnapShot.getDepositTxCount());
            } else {
                int realDepositTxCount = depositTxCountRealTime - depositTxCountGap;
                addressDailyAggregationRealTime.setDepositTxCount(realDepositTxCount);
            }
            if (withdrawTxCountRealTime < withdrawTxCountSnapshot) {
                addressDailyAggregationRealTime.setWithdrawTxCount(addressDailyAggregationSnapShot.getWithdrawTxCount());
            } else {
                int realWithdrawTxCount = withdrawTxCountRealTime - withdrawTxCountGap;
                addressDailyAggregationRealTime.setWithdrawTxCount(realWithdrawTxCount);
            }
            if (depositAmountRealTime.compareTo(depositAmountSnapshot) < 0) {
                addressDailyAggregationRealTime.setDepositAmount(addressDailyAggregationSnapShot.getDepositAmount());
            } else {
                BigDecimal realDepositAmount = depositAmountRealTime.subtract(depositAmountGap);
                addressDailyAggregationRealTime.setDepositAmount(realDepositAmount);
            }
            if (withdrawAmountRealTime.compareTo(withdrawAmountSnapshot) < 0) {
                addressDailyAggregationRealTime.setWithdrawAmount(addressDailyAggregationSnapShot.getWithdrawAmount());
            } else {
                BigDecimal realWithdrawAmount = withdrawAmountRealTime.subtract(withdrawAmountGap);
                addressDailyAggregationRealTime.setWithdrawAmount(realWithdrawAmount);
            }
            addressDailyAggregationMapper.updateTxCount(addressDailyAggregationRealTime);
        }

        if (isOep) {
            AddressDailyAggregation addressDailyAggregationRealTimeForOep = addressDailyAggregationMapper.selectTxCount(address, contractHash, -2147483648);
            if (addressDailyAggregationRealTimeForOep == null) {
                addressDailyAggregationRealTimeForOep = new AddressDailyAggregation();
                BeanUtils.copyProperties(addressDailyAggregationRealTime, addressDailyAggregationRealTimeForOep);
                addressDailyAggregationRealTimeForOep.setDateId(-2147483648);
                addressDailyAggregationMapper.insertSelective(addressDailyAggregationRealTimeForOep);
            } else {
                addressDailyAggregationRealTimeForOep.setDepositTxCount(addressDailyAggregationRealTime.getDepositTxCount());
                addressDailyAggregationRealTimeForOep.setWithdrawTxCount(addressDailyAggregationRealTime.getWithdrawTxCount());
                addressDailyAggregationRealTimeForOep.setDepositAmount(addressDailyAggregationRealTime.getDepositAmount());
                addressDailyAggregationRealTimeForOep.setWithdrawAmount(addressDailyAggregationRealTime.getWithdrawAmount());
                addressDailyAggregationMapper.updateTxCount(addressDailyAggregationRealTimeForOep);
            }
        }
        log.info("fixAddressTxCount success:{},{}", address, contractHash);
    }
}
