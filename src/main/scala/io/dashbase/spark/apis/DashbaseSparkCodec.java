package io.dashbase.spark.apis;

public abstract class DashbaseSparkCodec<Req, Res> {
    public abstract TimesliceSelector<Req> timesliceSelector();

    public abstract ResponseMerger<Res> responseMerger();

    public abstract TimesliceQuerier<Req, Res> timesliceQuerier();
}
