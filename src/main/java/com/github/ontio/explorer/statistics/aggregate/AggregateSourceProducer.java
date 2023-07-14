package com.github.ontio.explorer.statistics.aggregate;

import com.github.ontio.explorer.statistics.aggregate.model.ReSync;
import com.github.ontio.explorer.statistics.aggregate.model.Tick;
import com.github.ontio.explorer.statistics.aggregate.model.TransactionInfo;
import com.github.ontio.explorer.statistics.aggregate.support.DisruptorEventDispatcher;
import com.github.ontio.explorer.statistics.common.ParamsConfig;
import com.github.ontio.explorer.statistics.mapper.ContractMapper;
import com.github.ontio.explorer.statistics.mapper.CurrentMapper;
import com.github.ontio.explorer.statistics.mapper.TxDetailMapper;
import com.github.ontio.explorer.statistics.model.Contract;
import com.github.ontio.explorer.statistics.model.TxDetail;
import com.google.common.util.concurrent.RateLimiter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.RowBounds;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.entity.Example;

import javax.annotation.PostConstruct;
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

}
