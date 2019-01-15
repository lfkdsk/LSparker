package io.dashbase.spark.apis;

import java.util.Collections;
import java.util.Set;

public interface TimesliceQuerier<Req, Res> {
    Res query(Req request, Set<String> timeslices) throws Exception;
    default Res query(Req request, String tiemslice) throws Exception {
        return query(request, Collections.singleton(tiemslice));
    }
}
