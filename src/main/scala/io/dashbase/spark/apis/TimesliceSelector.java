package io.dashbase.spark.apis;

import java.util.Set;
import java.util.function.Function;

public interface TimesliceSelector<R> extends Function<R, Set<String>> {
}
