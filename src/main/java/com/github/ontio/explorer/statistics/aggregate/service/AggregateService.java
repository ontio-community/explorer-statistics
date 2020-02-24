package com.github.ontio.explorer.statistics.aggregate.service;

import com.github.ontio.explorer.statistics.aggregate.AggregateContext;
import com.github.ontio.explorer.statistics.aggregate.model.AddressAggregate;
import com.github.ontio.explorer.statistics.aggregate.model.Aggregate;
import com.github.ontio.explorer.statistics.aggregate.model.AggregateKey;
import com.github.ontio.explorer.statistics.aggregate.model.AggregateSnapshot;
import com.github.ontio.explorer.statistics.aggregate.model.ContractAggregate;
import com.github.ontio.explorer.statistics.aggregate.model.TokenAggregate;
import com.github.ontio.explorer.statistics.mapper.AddressDailyAggregationMapper;
import com.github.ontio.explorer.statistics.mapper.ContractDailyAggregationMapper;
import com.github.ontio.explorer.statistics.mapper.CurrentMapper;
import com.github.ontio.explorer.statistics.mapper.TokenDailyAggregationMapper;
import com.github.ontio.explorer.statistics.model.AddressDailyAggregation;
import com.github.ontio.explorer.statistics.model.ContractDailyAggregation;
import com.github.ontio.explorer.statistics.model.TokenDailyAggregation;
import lombok.RequiredArgsConstructor;
import org.apache.ibatis.session.RowBounds;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tk.mybatis.mapper.entity.Example;

import java.util.List;

/**
 * @author LiuQi
 */
@Service
@RequiredArgsConstructor
public class AggregateService {

	private static final int DATABASE_BATCH_SIZE = 500;

	private final AggregateContext context;

	private final CurrentMapper currentMapper;

	private final AddressDailyAggregationMapper addressDailyAggregationMapper;

	private final TokenDailyAggregationMapper tokenDailyAggregationMapper;

	private final ContractDailyAggregationMapper contractDailyAggregationMapper;

	public Aggregate<?, ?> createBaselineAggregate(AggregateKey key) {
		if (key instanceof AddressAggregate.AddressAggregateKey) {
			return createBaselineAggregate((AddressAggregate.AddressAggregateKey) key);
		} else if (key instanceof TokenAggregate.TokenAggregateKey) {
			return createBaselineAggregate((TokenAggregate.TokenAggregateKey) key);
		} else if (key instanceof ContractAggregate.ContractAggregateKey) {
			return createBaselineAggregate((ContractAggregate.ContractAggregateKey) key);
		} else {
			throw new IllegalArgumentException("unsupported key type " + key.getClass());
		}
	}

	private AddressAggregate createBaselineAggregate(AddressAggregate.AddressAggregateKey key) {
		Example example = new Example(AddressDailyAggregation.class);
		Example.Criteria criteria = example.and();
		criteria.andEqualTo("address", key.getAddress()).andEqualTo("tokenContractHash", key.getTokenContractHash());
		example.orderBy("dateId").desc();
		List<AddressDailyAggregation> data = addressDailyAggregationMapper.selectByExampleAndRowBounds(example,
				new RowBounds(0, 1));
		AddressDailyAggregation baseline = data == null || data.isEmpty() ? null : data.get(0);

		criteria.andEqualTo("dateId", 0);
		AddressDailyAggregation total = addressDailyAggregationMapper.selectOneByExample(example);

		AddressAggregate aggregate = new AddressAggregate(context, key);
		aggregate.populate(baseline, total);
		return aggregate;
	}

	private TokenAggregate createBaselineAggregate(TokenAggregate.TokenAggregateKey key) {
		Example example = new Example(TokenDailyAggregation.class);
		Example.Criteria criteria = example.and();
		criteria.andEqualTo("tokenContractHash", key.getTokenContractHash());
		example.orderBy("dateId").desc();
		List<TokenDailyAggregation> data = tokenDailyAggregationMapper.selectByExampleAndRowBounds(example, new RowBounds(0, 1));
		TokenDailyAggregation baseline = data == null || data.isEmpty() ? null : data.get(0);

		criteria.andEqualTo("dateId", 0);
		TokenDailyAggregation total = tokenDailyAggregationMapper.selectOneByExample(example);

		TokenAggregate aggregate = new TokenAggregate(context, key);
		aggregate.populate(baseline, total);
		return aggregate;
	}

	private ContractAggregate createBaselineAggregate(ContractAggregate.ContractAggregateKey key) {
		Example example = new Example(ContractDailyAggregation.class);
		Example.Criteria criteria = example.and();
		criteria.andEqualTo("contractHash", key.getContractHash()).andEqualTo("tokenContractHash", key.getTokenContractHash());
		example.orderBy("dateId").desc();
		List<ContractDailyAggregation> data = contractDailyAggregationMapper.selectByExampleAndRowBounds(example,
				new RowBounds(0, 1));
		ContractDailyAggregation baseline = data == null || data.isEmpty() ? null : data.get(0);

		criteria.andEqualTo("dateId", 0);
		ContractDailyAggregation total = contractDailyAggregationMapper.selectOneByExample(example);

		ContractAggregate aggregate = new ContractAggregate(context, key);
		aggregate.populate(baseline, total);
		return aggregate;
	}

	@Transactional
	public void saveAggregateSnapshot(AggregateSnapshot snapshot) {
		List<AddressDailyAggregation> addressAggregations = snapshot.getAddressAggregations();
		if (addressAggregations.size() > 0) {
			for (int i = 0; (DATABASE_BATCH_SIZE * i) < addressAggregations.size(); i++) {
				int from = DATABASE_BATCH_SIZE * i;
				int to = Math.min(DATABASE_BATCH_SIZE * (i + 1), addressAggregations.size());
				addressDailyAggregationMapper.batchSave(addressAggregations.subList(from, to));
			}
		}

		List<TokenDailyAggregation> tokenAggregations = snapshot.getTokenAggregations();
		if (tokenAggregations.size() > 0) {
			for (int i = 0; (DATABASE_BATCH_SIZE * i) < tokenAggregations.size(); i++) {
				int from = DATABASE_BATCH_SIZE * i;
				int to = Math.min(DATABASE_BATCH_SIZE * (i + 1), tokenAggregations.size());
				tokenDailyAggregationMapper.batchSave(tokenAggregations.subList(from, to));
			}
		}

		List<ContractDailyAggregation> contractAggregations = snapshot.getContractAggregations();
		if (contractAggregations.size() > 0) {
			for (int i = 0; (DATABASE_BATCH_SIZE * i) < contractAggregations.size(); i++) {
				int from = DATABASE_BATCH_SIZE * i;
				int to = Math.min(DATABASE_BATCH_SIZE * (i + 1), contractAggregations.size());
				contractDailyAggregationMapper.batchSave(contractAggregations.subList(from, to));
			}
		}

		currentMapper.saveLastStatTxTime(snapshot.getLastTxTime());
	}

}