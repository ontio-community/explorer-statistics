package com.github.ontio.explorer.statistics.aggregate.ranking;

import com.github.ontio.explorer.statistics.aggregate.AggregateContext;
import com.github.ontio.explorer.statistics.aggregate.model.TransactionInfo;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

/**
 * @author LiuQi
 */
public class CompositeRanking implements Ranking {

    private final AggregateContext context;

    @Getter
    private final RankingDuration duration;

    private Map<String, AddressRanking> addressRankings;

    private Map<String, TokenRanking> tokenRankings;

    public CompositeRanking(AggregateContext context, RankingDuration duration) {
        this.context = context;
        this.duration = duration;
        this.addressRankings = new HashMap<>();
        this.tokenRankings = new HashMap<>();
    }

    @Override
    public void rank(TransactionInfo transactionInfo) {
        String from = transactionInfo.getFromAddress();
        if (!context.isGovernor(from)) {
            getAddressRanking(from).rank(transactionInfo);
        }
        if (!transactionInfo.isSelfTransaction()) {
            String to = transactionInfo.getToAddress();
            if (!context.isGovernor(to)) {
                getAddressRanking(to).rank(transactionInfo);
            }
        }

        String tokenContractHash = transactionInfo.getContractHash();
        if (context.isOep4Contract(tokenContractHash)) {
            getTokenRanking(tokenContractHash).rank(transactionInfo);
        }
    }

    private AddressRanking getAddressRanking(String address) {
        return addressRankings.computeIfAbsent(address, key -> new AddressRanking(context, duration, key));
    }

    private TokenRanking getTokenRanking(String tokenContractHash) {
        return tokenRankings.computeIfAbsent(tokenContractHash, key -> new TokenRanking(duration, key));
    }

    public AddressRanking[] getAddressRankings() {
        return addressRankings.values().toArray(new AddressRanking[0]);
    }

    public TokenRanking[] getTokenRankings() {
        return tokenRankings.values().toArray(new TokenRanking[0]);
    }

}
