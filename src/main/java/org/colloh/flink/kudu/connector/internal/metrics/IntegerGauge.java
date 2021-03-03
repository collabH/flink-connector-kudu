package org.colloh.flink.kudu.connector.internal.metrics;

import org.apache.flink.metrics.Gauge;

/**
 * @fileName: CacheGauge.java
 * @description: CacheGauge.java类说明
 * @author: by echo huang
 * @date: 2020/12/31 4:28 下午
 */
public class IntegerGauge implements Gauge<Integer> {
    private Integer count;

    public IntegerGauge() {
    }

    public IntegerGauge(Integer count) {
        this.count = count;
    }

    @Override
    public Integer getValue() {
        return this.count;
    }
}
