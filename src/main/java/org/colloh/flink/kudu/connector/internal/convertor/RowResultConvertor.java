package org.colloh.flink.kudu.connector.internal.convertor;

import org.apache.kudu.client.RowResult;

import java.io.Serializable;

/**
 * @fileName: RowConvertor.java
 * @description: Row转换器
 * @author: by echo huang
 * @date: 2021/3/3 3:14 下午
 */
public interface RowResultConvertor<T> extends Serializable {

    /**
     * 将Kudu RowResult转换成对应T格式
     *
     * @param row kudu记录格式
     * @return {@link T}
     */
    T convertor(RowResult row);
}
