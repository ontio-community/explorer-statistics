/*
 * Copyright (C) 2018 The ontology Authors
 * This file is part of The ontology library.
 * The ontology is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * The ontology is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License
 * along with The ontology.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.github.ontio.explorer.statistics.common;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

public class Constants {
    public static final int GENESIS_TIME = 1530316800;

    public static final int ONE_DAY_IN_SEC = 86400;

    public static final String ONT = "ont";

    public static final String ONG = "ong";

    public static final String ONG_CONTRACT_HASH = "0200000000000000000000000000000000000000";

    public static final String EVM_PREFIX = "0x";

    public static final BigDecimal ZERO = new BigDecimal("0");

    public static final String ADDR_DAILY_SUMMARY_NATIVETYPE = "0000000000000000000000000000000000000000";

    public static final Integer UTC_20210801 = 1627776000;

    public static final BigDecimal ONE_HUNDRED = new BigDecimal("100");

    public static final BigDecimal RECENT_BLOCK_VELOCITY = new BigDecimal(2500);

    public static final List<String> TOKEN_TYPES = Arrays.asList("oep4", "oep5", "oep8", "orc20", "orc721", "orc1155");

    // 地址数量统计排除00地址和07地址
    public static final List<String> EXCLUDE_ADDRESS_LIST = Arrays.asList("00", "AFmseVrdL9f9oyCzZefL9tG6UbvhPbdYzM", "0x0000000000000000000000000000000000000000", "AFmseVrdL9f9oyCzZefL9tG6UbviEH9ugK", "0x0000000000000000000000000000000000000007");
}
