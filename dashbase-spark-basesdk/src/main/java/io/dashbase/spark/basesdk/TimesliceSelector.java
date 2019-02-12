package io.dashbase.spark.basesdk;/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.Filter;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public interface TimesliceSelector {
    Set<String> timeSliceSelector(SQLContext sqlContext, String path, List<Filter> filters);

    default Set<String> timeSliceSelector(SQLContext sqlContext, String path) {
        return timeSliceSelector(sqlContext, path, Collections.emptyList());
    }

    default Set<String> timeSliceSelector(SQLContext sqlContext, List<Filter> filters) {
        String searchPath = sqlContext.getConf(DashbaseConstants.DASHBASE_SEARCH_PATH);
        if (searchPath == null) {
            throw new IllegalArgumentException("need search path as params in conf with key: [ " + DashbaseConstants.DASHBASE_SEARCH_PATH + " ]");
        }

        return timeSliceSelector(sqlContext, searchPath, filters);
    }
}
