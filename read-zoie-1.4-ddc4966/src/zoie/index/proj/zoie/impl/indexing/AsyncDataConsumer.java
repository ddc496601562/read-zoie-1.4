package proj.zoie.impl.indexing;

import java.util.Collection;
import java.util.LinkedList;

import org.apache.log4j.Logger;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.ZoieException;
/**
 * 数据的消费者，由ZoieSystem来继承，实际的的数据消费者 ，由StreamDataProvider.start()方法启动的线程DataThread来调用
 * 其中也有一个DataConsumer<V> _consumer，
 * @author cctv
 * @param <V>
 */
public class AsyncDataConsumer<V> implements DataConsumer<V>
{
  private static final Logger log = Logger.getLogger(AsyncDataConsumer.class);
  
  private ConsumerThread _consumerThread;
  private DataConsumer<V> _consumer;
  private long _currentVersion;
  private long _bufferedVersion;
  private LinkedList<DataEvent<V>> _batch;
  private int _batchSize;

  public AsyncDataConsumer()
  {
    _currentVersion = -1L;
    _bufferedVersion = -1L;
    _batch = new LinkedList<DataEvent<V>>();
    _batchSize = 1; // default
    _consumerThread = null;
  }
  //如果这个方法不调用，则不能启动消费的进程_consumerThread 。
  public void start()
  {
    _consumerThread = new ConsumerThread();
    _consumerThread.setDaemon(true);
    _consumerThread.start();
  }
  
  public void stop()
  {
    _consumerThread.terminate();
  }
  
  public void setDataConsumer(DataConsumer<V> consumer)
  {
    synchronized(this)
    {
      _consumer = consumer;
    }
  }
  
  public void setBatchSize(int batchSize)
  {
    synchronized(this)
    {
      _batchSize = Math.max(1, batchSize);
    }
  }
  
  public int getBatchSize()
  {
    synchronized(this)
    {
      return _batchSize;
    }
  }
  
  public int getCurrentBatchSize()
  {
    synchronized(this)
    {
      return (_batch != null ? _batch.size() : 0);
    }
  }
  
  public long getCurrentVersion()
  {
    synchronized(this)
    {
      return _currentVersion;
    }
  }
  
  public void flushEvents(long timeout) throws ZoieException
  {
    syncWthVersion(timeout, _bufferedVersion);
  }
  
  public void syncWthVersion(long timeInMillis, long version) throws ZoieException
  {
    long now = System.currentTimeMillis();
    long due = now + timeInMillis;
    
    if(_consumerThread == null) throw new ZoieException("not running");
    
    synchronized(this)
    {
      while(_currentVersion < version)
      {
        if(now >= due)
        {
          throw new ZoieException("sync timed out");
        }
        try
        {
          this.wait(due - now);
        }
        catch(InterruptedException e)
        {
          log.warn(e.getMessage(), e);
        }
        now = System.currentTimeMillis();
      }
    }
  }
  /**
   * 别的对象调用此方法将要消费的数据传递进来 。
   * 该方法会判断LinkedList<DataEvent<V>> _batch中是否装满数据，若是装满，则休眠这个方法，然后会唤醒flushBuffer()去将_batch中数据消费完
   */
  @Override
  public void consume(Collection<DataEvent<V>> data) throws ZoieException
  {
    if (data == null || data.size() == 0) return;
    
    synchronized(this)
    {
      while(_batch.size() >= _batchSize)  //如果数据满了，则休眠自己一会让其他的方法（flushBuffer()）去flush掉数据到真正的ZoieSystem中，等使用完数据该方法会继续执行下去
      {
        if(_consumerThread == null || !_consumerThread.isAlive() || _consumerThread._stop)
        {
          throw new ZoieException("consumer thread has stopped");
        }
        try
        {
          this.wait();
        }
        catch (InterruptedException e)
        {
        }
      }
      for(DataEvent<V> event : data)
      {
        _bufferedVersion = Math.max(_bufferedVersion, event.getVersion());
        _batch.add(event);
      }
      this.notifyAll(); // wake up the thread waiting in flushBuffer()
    }
  }
  //该对象启动时自动创建一个守护进程，然后不停的来掉这个方法
  /**
   * 该方法会不停的被一个守护进程ConsumerThread来调用，每次取"吃掉"外界传来的在LinkedList<DataEvent<V>> _batch中存的数据，
   * 若是调用时_batch中没有数据，则等待该方法，唤醒consume()去装数据，装完一次数据后在唤醒自己，去“吃掉”这部分数据
   */
  protected final void flushBuffer()
  {
    long version;
    LinkedList<DataEvent<V>> currentBatch;
    
    synchronized(this)
    {
      while(_batch.size() == 0)    //如果等于0的话，则休眠自己，让别的线程（consume()）使用，去装数据 ，装完数据会后会继续往下执行
      {
        if(_consumerThread._stop) return;
        try
        {
          this.wait();
        }
        catch (InterruptedException e)
        {
        }
      }
      version = Math.max(_currentVersion, _bufferedVersion);
      currentBatch = _batch;
      _batch = new LinkedList<DataEvent<V>>();
      
      this.notifyAll(); // wake up the thread waiting in addEventToBuffer()
    }
    //消费数据
    if(_consumer != null)
    {
      try
      {
        _consumer.consume(currentBatch);
      }
      catch (Exception e)
      {
        log.error(e.getMessage(), e);
      }
    }
    
    synchronized(this)
    {
      _currentVersion = version;
      this.notifyAll(); // wake up the thread waiting in syncWthVersion()
    }    
  }
//内启动的线程，不停的调用父对象的flushBuffer（）
  private final class ConsumerThread extends IndexingThread
  {
    boolean _stop = false;
    
    ConsumerThread()
    {
      super("ConsumerThread");
    }
    
    public void terminate()
    {
      _stop = true;
      synchronized(AsyncDataConsumer.this)
      {
        AsyncDataConsumer.this.notifyAll();
      }
    }
    
    public void run()
    {
      while(!_stop)
      {
        flushBuffer();
      }
    }
  }
}
