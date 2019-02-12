package io.dashbase.spark.basesdk;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.Set;

public interface TimesliceQuerier {
    RDD<Row> query(SQLContext sqlContext, StructType type, String query, Set<String> timeslices) throws Exception;

    default RDD<Row> query(SQLContext sqlContext, StructType type, String query, String tiemslice) throws Exception {
        return query(sqlContext, type, query, Collections.singleton(tiemslice));
    }
}