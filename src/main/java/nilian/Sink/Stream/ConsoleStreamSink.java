package nilian.Sink.Stream;

import java.util.concurrent.BlockingQueue;

public class ConsoleStreamSink<T> implements StreamSinkFunction {

    BlockingQueue<T> dataQueue ;

    public ConsoleStreamSink(BlockingQueue<T> dataQueue) {
        this.dataQueue = dataQueue;
    }

    @Override
    public void sinkData() {
        while (true) {
            try {
                String data = dataQueue.take().toString();
                System.out.println(data);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
