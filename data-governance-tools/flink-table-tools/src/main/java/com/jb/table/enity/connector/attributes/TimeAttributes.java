package com.jb.table.enity.connector.attributes;

import com.jb.table.enums.IntervalEnum;
import com.jb.table.enums.TimeAttributeEnum;
import lombok.Getter;

/**
 * @author zhaojb
 * <p>
 * FlinkTable时间属性:
 * <p>
 * 事件属性：允许表程序根据每条记录中的时间戳生成结果，即使发生乱序或延迟事件也能获得一致的结果。它还确保了从持久存储中
 * 读取记录时表程序结果的可重放性，是使用表DDL中的WATERMARK语句定义的。CREATE水印语句在现有事件时间字段上定义水印生成表达式，将事件时间字段标记为事件时间属性。
 * <p>
 * 处理属性：允许表程序根据本地机器的时间产生结果。这是最简单的时间概念，但会产生不确定的结果。处理时间不需要时间戳提取或水印生成。
 */
@Getter
public class TimeAttributes {

    /**
     * 时间属性类型:事件属性,处理属性
     */
    private TimeAttributeEnum timeAttributeType;

    private String timeAttribute;

    /**
     * 事件时间属性
     *
     * @param eventTimeColumn 事件时间字段
     * @param interval        间隔时间
     * @param intervalType    间隔单位
     * @return
     */
    public static TimeAttributes eventTimeBuilder(String eventTimeColumn,Integer interval,
                                                  IntervalEnum intervalType) {

        // WATERMARK FOR user_action_time AS user_action_time - INTERVAL '5' SECOND

        return new TimeAttributes(TimeAttributeEnum.EVENT_TIME,
                                  ", WATERMARK FOR " +
                                          eventTimeColumn +
                                          " AS " +
                                          eventTimeColumn +
                                          " - INTERVAL " +
                                          "'" +
                                          interval +
                                          "' " +
                                          intervalType.name()
        );
    }

    /**
     * 处理时间属性
     *
     * @param eventTimeColumn 处理时间属性字段
     */
    public static TimeAttributes processingTimeBuilder(String eventTimeColumn) {

        // user_action_time AS PROCTIME()

        return new TimeAttributes(TimeAttributeEnum.PROCESSING_TIME,
                                  "," +
                                          eventTimeColumn +
                                          " AS PROCTIME() "
        );
    }

    private TimeAttributes(TimeAttributeEnum timeAttributeType,String timeAttribute) {
        this.timeAttributeType = timeAttributeType;
        this.timeAttribute = timeAttribute;
    }
}
