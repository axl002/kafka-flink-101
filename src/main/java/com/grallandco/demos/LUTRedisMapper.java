package com.grallandco.demos;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;


public class LUTRedisMapper implements RedisMapper<String> {

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.LPUSH, "priceLUT");
    }

    @Override
    public String getKeyFromData(String data) {
        return "priceLUT";
    }

    @Override
    public String getValueFromData(String data) {
        return data;
    }
}

