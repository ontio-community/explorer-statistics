package com.github.ontio.explorer.statistics.aggregate;

import com.github.ontio.explorer.statistics.aggregate.model.TransactionInfo;
import com.github.ontio.explorer.statistics.aggregate.ranking.RankingDuration;
import com.github.ontio.explorer.statistics.aggregate.support.DateIdUtil;
import com.github.ontio.explorer.statistics.mapper.TxDetailMapper;
import com.github.ontio.explorer.statistics.model.TxDetail;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tk.mybatis.mapper.entity.Example;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * @author LiuQi
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class RankingSourceProducer {

	private static final int BLOCK_BATCH_SIZE = 1000;

	private final TxDetailMapper txDetailMapper;

	private final TransactionInfoRanker ranker;

	private volatile int rankingDateId;

	@Scheduled(cron = "0 5 * * * *")
	public void produceTransactionInfoForRanking() {
		DateTime now = DateTime.now(DateTimeZone.UTC);
		DateTime dt = now.withTime(now.getHourOfDay(), 0, 0, 0);
		int endTxTime = (int) (dt.getMillis() / 1000);
		int rankingDateId = DateIdUtil.parseDateId(endTxTime);
		RankingDuration duration = (dt.getHourOfDay() == 0 && rankingDateId > this.rankingDateId)
				? RankingDuration.SEVEN_DAYS
				: RankingDuration.ONE_DAY;
		int beginTxTime = duration.getTxTimeBarrier(dt);

		Integer beginBlockHeight = txDetailMapper.findFirstBlockHeightAfterTxTime(beginTxTime);
		if (beginBlockHeight == null) {
			log.warn("no new tx detail found after {}", new Date(beginTxTime * 1000L));
			return;
		}
		Integer endBlockHeight = txDetailMapper.findLastBlockHeightBeforeTxTime(endTxTime);

		try {
			log.info("producing transaction info for {} rankings", duration);
			duration.involved().stream().map(d -> new RankingDuration.Begin(d, d.getTxTimeBarrier(dt))).forEach(ranker::publish);

			while (beginBlockHeight < endBlockHeight) {
				Example example = new Example(TxDetail.class);
				example.and()
						.andGreaterThan("blockHeight", beginBlockHeight)
						.andLessThanOrEqualTo("blockHeight", Math.min(beginBlockHeight + BLOCK_BATCH_SIZE, endBlockHeight))
						.andIn("eventType", Arrays.asList(2, 3));
				example.orderBy("blockHeight").orderBy("blockIndex").orderBy("txIndex");
				List<TxDetail> details = txDetailMapper.selectByExample(example);

				if (details == null || details.isEmpty()) {
					beginBlockHeight += BLOCK_BATCH_SIZE;
					continue;
				}

				for (TxDetail detail : details) {
					TransactionInfo transactionInfo = TransactionInfo.wrap(detail);
					ranker.publish(transactionInfo);
					beginBlockHeight = detail.getBlockHeight();
				}
			}
		} catch (Exception e) {
			log.error("error submitting transaction info for ranking", e);
		} finally {
			duration.involved().stream().map(RankingDuration.End::new).forEach(ranker::publish);
		}

		this.rankingDateId = rankingDateId;
	}

}
