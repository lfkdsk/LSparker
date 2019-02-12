package io.dashbase.spark.apis;

import org.apache.spark.SparkContext;

import java.util.Set;
import java.util.function.Function;

public interface TimesliceSelector<R> extends Function<R, Set<String>> {
    default void initial(SparkContext sc) { }
}
