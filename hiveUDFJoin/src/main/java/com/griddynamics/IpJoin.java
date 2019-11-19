package com.griddynamics;

import org.apache.hadoop.hive.ql.exec.UDF;


import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class IpJoin extends UDF {
    public Boolean evaluate(String one, String two) {
        return netMatch(one, two);
    }

    public  boolean netMatch(String addr, String addr1){ //addr is subnet address and addr1 is ip address. Function will return true, if addr1 is within addr(subnet)

        String[] parts = addr.split("/");
        String ip = parts[0];
        int prefix;

        if (parts.length < 2) {
            prefix = 0;
        } else {
            prefix = Integer.parseInt(parts[1]);
        }

        Inet4Address a =null;
        Inet4Address a1 =null;
        try {
            a = (Inet4Address) InetAddress.getByName(ip);
            a1 = (Inet4Address) InetAddress.getByName(addr1);
        } catch (UnknownHostException e){}

        byte[] b = a.getAddress();
        int ipInt = ((b[0] & 0xFF) << 24) |
                ((b[1] & 0xFF) << 16) |
                ((b[2] & 0xFF) << 8)  |
                ((b[3] & 0xFF) << 0);

        byte[] b1 = a1.getAddress();
        int ipInt1 = ((b1[0] & 0xFF) << 24) |
                ((b1[1] & 0xFF) << 16) |
                ((b1[2] & 0xFF) << 8)  |
                ((b1[3] & 0xFF) << 0);

        int mask = ~((1 << (32 - prefix)) - 1);

        if ((ipInt & mask) == (ipInt1 & mask)) {
            return true;
        }
        else {
            return false;
        }
    }
}
