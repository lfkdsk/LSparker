package io.dashbase.spark.basesdk;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.Collections;
import java.util.Set;

public interface TimesliceQuerier {
    Row query(SQLContext sqlContext, String query, Set<String> timeslices) throws Exception;

    default Row query(SQLContext sqlContext, String query, String tiemslice) throws Exception {
        return query(sqlContext, query, Collections.singleton(tiemslice));
    }
}