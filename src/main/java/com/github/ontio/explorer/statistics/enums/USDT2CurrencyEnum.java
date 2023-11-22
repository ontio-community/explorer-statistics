package com.github.ontio.explorer.statistics.enums;

/**
 * 货币汇率枚举
 *
 */
public enum USDT2CurrencyEnum {
    /**
     * 美元
     */
    USD("USD"),
    /**
     * 新加坡元
     */
    SGD("SGD"),
    /**
     * 泰铢
     */
    THB("THB"),
    /**
     * 韩元
     */
    KRW("KRW"),
    /**
     * 印尼卢比
     */
    IDR("IDR"),
    ;

    /**
     * 货币名称
     */
    private String name;

    USDT2CurrencyEnum(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


}
