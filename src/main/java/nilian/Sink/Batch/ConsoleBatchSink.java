package nilian.Sink.Batch;
public class ConsoleBatchSink<T> implements SinkBatchFunction<T> {
    @Override
    public void sinkData(T data) {
        System.out.println(data);
    }
}
