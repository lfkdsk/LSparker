package io.dashbase.spark.apis;

import java.util.Set;

public interface ResponseMerger<R> {
    R merge(Set<R> resultSet) throws Exception;
}
