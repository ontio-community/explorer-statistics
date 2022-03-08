package com.github.ontio.explorer.statistics.util;

import com.github.ontio.common.Address;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HttpUtil {

    public static String ethAddrToOntAddr(String ethAddr) {
        ethAddr = ethAddr.substring(2);
        Address parse = Address.parse(ethAddr);
        return parse.toBase58();
    }

}
