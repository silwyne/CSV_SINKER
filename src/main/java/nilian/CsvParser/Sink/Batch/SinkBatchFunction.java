package nilian.CsvParser.Sink.Batch;

public interface SinkBatchFunction<T> {
    public void sinkData(T data) ;
}
