package dmq.test.logging.log4j;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

// 设定定时器，周期性的批量处理数据，直到close
public class TimedBuffer<T> implements Runnable {
    private static final Logger LOGGER= LoggerFactory.getLogger(TimedBuffer.class);

    public static final int MAX_CAPACITY= 10000;    // 缓存最大容量，超出容量的数据自动丢弃
    public static final int THRESHOLD_SIZE= 1000;   // 缓存达到一定数量之后，立刻处理（不影响定时处理）
    public static final int THRESHOLD_TIME= 1000;   // 每隔一定毫秒的时间，定期处理一次
    public static final int MAX_THREADS= 1;         // 用于定时服务的线程池大小
    public static final boolean FIX_DELAY= true;    // 是否使用固定延时（距上次多久再执行），而不是固定频率（每多久执行）

    private ScheduledExecutorService bufferService= null;
    private List<T> bufferList= new LinkedList<>();
    private long countDropped= 0;// 当缓存满载之后，记录丢弃掉数据的次数
    private BufferHandler bufferHandler;
    private BufferDroppedNotify bufferDroppedNotify;
    public TimedBuffer(BufferHandler<T> handler) {
        this(MAX_CAPACITY, THRESHOLD_SIZE, THRESHOLD_TIME, MAX_THREADS, FIX_DELAY, handler, null);
    }
    private final int maxCapacity, thresholdSize, thresholdTime;
    public TimedBuffer(int maxCapacity, int thresholdSize, int thresholdTime, int maxThreads, boolean fixDelay,
                       BufferHandler<T> handler, BufferDroppedNotify notify) {
        this.maxCapacity= maxCapacity;
        this.thresholdSize= thresholdSize;
        this.thresholdTime= thresholdTime;
        bufferHandler= handler;
        bufferDroppedNotify= notify;
        startService(maxThreads, fixDelay);
        //installShutdownHook();
    }

    // 当程序退出时，最大程度处理缓存内的数据
    private void installShutdownHook() {
        final BufferHandler<T> bufferHandler= this.bufferHandler;
        final TimedBuffer<T> timedBuffer= this;
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                List<T> list= timedBuffer.take();
                int size= list.size();
                if(size > 0) {
                    //System.out.printf("writing buffered data before shutdown... (size= %d)\n", size);
                    LOGGER.trace("writing buffered data before shutdown... (size= {})", size);
                    bufferHandler.processBuffer(list);
                    //System.out.printf("written %d on shutdown.\n", size);
                    LOGGER.trace("written {} on shutdown.", size);
                }
            }
        }));
    }

    // 关闭定时服务之后，周期性处理数据功能失效
    public void close() {
        LOGGER.debug("Close timed buffer .");
        fireTiming();
        stopService(false, 5);
    }
    public void close(Runnable closeHandler) {
        close();
        if(closeHandler != null)
            closeHandler.run();
    }

    public synchronized boolean put(T t) {
        int size= bufferList.size();
        if(size >= maxCapacity) {
            countDropped++;
            //System.out.printf("Buffer is full, dropping data! countDropped= %d\n", countDropped);
            LOGGER.trace("Buffer is full, dropping data! countDropped= {}", countDropped);
            return false;
        }

        bufferList.add(t);
        // 达到限定的数量时，立刻处理
        if(size+1 >= thresholdSize)
            fireTiming();
        return true;
    }
    public /* synchronized */ List<T> take() {
        long dropped;
        BufferDroppedNotify notify;
        List<T> list;
        synchronized (this) {
            dropped= countDropped;
            notify= bufferDroppedNotify;
            list = bufferList;
            bufferList = new LinkedList<>();
            countDropped = 0;
        }// synchronized

        // 通常情况下，dropped应当为0，
        if (dropped > 0 && notify != null)
            notify.notifyBufferDroppped(dropped);
        return list;
    }

    public synchronized int count() {
        return bufferList.size();
    }
    public synchronized long countDropped() {
        return countDropped;
    }

    // 打开定时服务，忽略重复打开的情况
    private synchronized void startService(int threads, boolean fixDelay) {
        if(bufferService != null)
            return;
        if(bufferHandler == null)
            return;
        bufferService= Executors.newScheduledThreadPool(threads);
        if(fixDelay)
            bufferService.scheduleWithFixedDelay(this, thresholdTime, thresholdTime, TimeUnit.MILLISECONDS);
        else
            bufferService.scheduleAtFixedRate(this, thresholdTime, thresholdTime, TimeUnit.MILLISECONDS);
    }
    // 关闭定时服务，忽略重复关闭的情况
    private synchronized void stopService(boolean now, int waitCount) {
        if(bufferService == null)
            return;
        if(now)
            bufferService.shutdownNow();
        else {
            bufferService.shutdown();
            try {
                for(int i=0; i<waitCount; i++) {
                    LOGGER.debug("Waiting for termination ... (count={})", i);
                    if(bufferService.awaitTermination(1, TimeUnit.SECONDS))
                        break;
                }// for
                if(!bufferService.isTerminated())
                    bufferService.shutdownNow();
            } catch (InterruptedException e) {
                LOGGER.warn("Failed to wait for termination.", e);
            }
        }
        bufferService= null;
    }
    // 触发独立的定时器，已经打开的定义服务不受影响
    private synchronized void fireTiming() {
        final List<T> list= take();
        if(bufferHandler == null)
            return;
        bufferService.submit(new Runnable() {
            @Override
            public void run() {
                bufferHandler.processBuffer(list);
                LOGGER.trace("fireTiming: buffered data has been written! (size= {}, threshold= {})",
                        list.size(), thresholdSize);
            }
        });
    }


    // 处理缓存的接口，用于生成匿名函数
    public interface BufferHandler<T> {
        void processBuffer(List<T> bufferList);
    }
    // 从buffer中取出数据时，如果存在数据丢失的记录，告知丢失数量
    public interface BufferDroppedNotify<T> {
        void notifyBufferDroppped(long count);
    }

    @Override// Runnable
    public void run() {
        List<T> list= take();
        if(list.size() == 0)
            return;

        bufferHandler.processBuffer(list);
    }

}
