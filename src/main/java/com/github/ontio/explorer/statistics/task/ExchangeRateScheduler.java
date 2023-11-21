package com.github.ontio.explorer.statistics.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.ontio.explorer.statistics.common.ParamsConfig;
import com.github.ontio.explorer.statistics.enums.USDT2CurrencyEnum;
import com.github.ontio.explorer.statistics.mapper.CurrencyRateMapper;
import com.github.ontio.explorer.statistics.mapper.TokenPriceMapper;
import com.github.ontio.explorer.statistics.model.CurrencyRate;
import com.github.ontio.explorer.statistics.model.TokenPrice;
import com.github.ontio.explorer.statistics.util.HttpClientUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * @author lijie
 * @version 1.0
 * @date 2019/7/4
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ExchangeRateScheduler {
    @Autowired
    private CurrencyRateMapper currencyRateMapper;
    @Autowired
    private TokenPriceMapper tokenPriceMapper;
    @Autowired
    private ParamsConfig paramsConfig;

    private static final String CMC_QUOTES_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest";

    private static final String USDT_TOKENID = "825";

    private static final String ONT_ID = "2566";

    private static final String ONG_ID = "3217";

    @Scheduled(cron = "0 5/10 * * * *")
    public void getNativeTokenPrice() {
        log.info("start getNativeTokenPrice");
        JSONObject jsonObject = getLatestPriceFromCoinMarketCap(ONT_ID + "," + ONG_ID, "USD");
        if (jsonObject != null) {
            JSONObject data = jsonObject.getJSONObject("data");
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                try {
                    JSONObject tokenPriceJsonObject = (JSONObject) entry.getValue();
                    String symbol = tokenPriceJsonObject.getString("symbol");
                    int rank = tokenPriceJsonObject.getInteger("cmc_rank");
                    JSONObject quote = tokenPriceJsonObject.getJSONObject("quote").getJSONObject("USD");
                    BigDecimal token2UsdPrice = quote.getBigDecimal("price");
                    BigDecimal percentChange24h = quote.getBigDecimal("percent_change_24h");
                    if (percentChange24h == null) {
                        percentChange24h = BigDecimal.ZERO;
                    }

                    TokenPrice tokenPrice = new TokenPrice();
                    tokenPrice.setSymbol(symbol);
                    tokenPrice.setPrice(token2UsdPrice);
                    tokenPrice.setPercentChange24h(percentChange24h);
                    tokenPrice.setRank(rank);
                    int i = tokenPriceMapper.updateByPrimaryKeySelective(tokenPrice);
                    if (i == 0) {
                        tokenPriceMapper.insertSelective(tokenPrice);
                    }
                } catch (Exception e) {
                    log.error("getNativeTokenPrice error.", e);
                }
            }
        }
    }

    @Scheduled(cron = "0 0 * * * *")
    public void getUsdtExchangeRate() {
        log.info("start getUsdtExchangeRate");
        for (USDT2CurrencyEnum currencyEnum : USDT2CurrencyEnum.values()) {
            getUsdtExchangeRateFromCoinMarketCap(currencyEnum.getName());
        }
    }

    private void getUsdtExchangeRateFromCoinMarketCap(String currency) {
        JSONObject respObj = getLatestPriceFromCoinMarketCap(USDT_TOKENID, currency);
        if (respObj == null) {
            return;
        }
        JSONObject dataObj = respObj.getJSONObject("data");
        BigDecimal usdt2CurrencyPrice = dataObj.getJSONObject(USDT_TOKENID).getJSONObject("quote").getJSONObject(currency.toUpperCase()).getBigDecimal("price");
        CurrencyRate currencyRate = new CurrencyRate();
        currencyRate.setCurrencyName(currency);
        currencyRate.setPrice(usdt2CurrencyPrice);
        int i = currencyRateMapper.updateByPrimaryKeySelective(currencyRate);
        if (i == 0) {
            currencyRateMapper.insertSelective(currencyRate);
        }
    }


    /**
     * get latest price by coinmarketcap api
     *
     * @param requestedTokenIdsStr
     * @return
     * @throws Exception
     */
    private JSONObject getLatestPriceFromCoinMarketCap(String requestedTokenIdsStr, String currency) {
        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("convert", currency);
        paramMap.put("id", requestedTokenIdsStr);

        Map<String, Object> headerMap = new HashMap<>();
        headerMap.put("X-CMC_PRO_API_KEY", paramsConfig.getCmcApiKey());
        headerMap.put("Accept", "application/json");
        headerMap.put("Accept-Encoding", "deflate, gzip");

        try {
            String responseStr = HttpClientUtil.getRequest(CMC_QUOTES_URL, paramMap, headerMap);
            JSONObject responseObj = JSON.parseObject(responseStr);
            JSONObject status = responseObj.getJSONObject("status");
            // 0 is success code of CMC API
            if (status.getIntValue("error_code") != 0) {
                log.error("getLatestPriceFromCoinMarketCap error:{}", status.getString("error_message"));
                return null;
            }
            return responseObj;
        } catch (Exception e) {
            log.error("getLatestPriceFromCoinMarketCap getRequest error");
            return null;
        }
    }

}
