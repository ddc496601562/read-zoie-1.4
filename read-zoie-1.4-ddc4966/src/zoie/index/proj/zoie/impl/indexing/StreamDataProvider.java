package proj.zoie.impl.indexing;

import java.util.Collection;
import java.util.LinkedList;

import org.apache.log4j.Logger;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.DataProvider;
import proj.zoie.api.ZoieException;
import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.mbean.DataProviderAdminMBean;
/**\
 * StreamDataProvider中的start()方法，会启动一个来消费StreamDataProvider中数据的线程DataThread，DataThread通过
 * 不断调用StreamDataProvider.next()来取得数据，缓存在自己的List<DateEvent>中，当到达一定的数量（_batchSize）时统一交给ZoieSystem来索引消费
 * @author cctv
 *
 * @param <V>
 */
public abstract class StreamDataProvider<V> implements DataProvider<V>,DataProviderAdminMBean{
	private static final Logger log = Logger.getLogger(StreamDataProvider.class);
	
	private int _batchSize;      //每次处理的批量，DataProvider的处理线程DataThread中缓存的数据的数量，
	private DataConsumer<V> _consumer; //消费数据的消费者，一般注入的是ZoieSystem（本身是个消费者）
	private DataThread<V> _thread;     //消费DataProvider中数据的线程
	
	public StreamDataProvider()
	{
		_batchSize=1;
		_consumer=null;
	}
	
	public void setDataConsumer(DataConsumer<V> consumer)
	{
	  _consumer=consumer;	
	}

	public abstract DataEvent<V> next();   //实现者继承的方法，取数据，在这个方法里面定义自定义的数据来源
	
	protected abstract void reset();       
	
	public int getBatchSize() {
		return _batchSize;
	}

	public void pause() {
		if (_thread != null)
		{
			_thread.pauseDataFeed();
		}
	}

	public void resume() {
		if (_thread != null)
		{
			_thread.resumeDataFeed();
		}
	}

	public void setBatchSize(int batchSize) {
		_batchSize=Math.max(1, batchSize);
	}
	
	public void stop()
	{
		if (_thread!=null && _thread.isAlive())
		{
			_thread.terminate();
			try {
				_thread.join();
			} catch (InterruptedException e) {
				log.warn("stopping interrupted");
			}
		}
	}

	public void start() {
		if (_thread==null || !_thread.isAlive())
		{
			reset();
			_thread = new DataThread<V>(this);
			_thread.start();
		}
	}
	
	public void syncWthVersion(long timeInMillis, long version) throws ZoieException
	{
	  _thread.syncWthVersion(timeInMillis, version);
	}
	/**
	 * 消费中DataProvider数据的线程 。
	 * 每次由DataProvider启动一个该线程的实例(假如当前没有该线程的实例)来将DataProvider中的数据消费到ZoieSystem中
	 * @author cctv
	 *
	 * @param <V>
	 */
	private static final class DataThread<V> extends Thread
	{
	    private Collection<DataEvent<V>> _batch;
		private long _currentVersion;   //已经从DataProvider取出来的DateEvent<V>的最大的_currentVersion
		private final StreamDataProvider<V> _dataProvider;
		private boolean _paused;       //通过该参数来暂停该线程的运行
		private boolean _stop;         //通过该参数来停止该线程的运行
		
		DataThread(StreamDataProvider<V> dataProvider)
		{
			super("Stream DataThread");
			setDaemon(false);
			_dataProvider = dataProvider;
			_currentVersion = 0L;
			_paused = false;
			_stop = false;
			_batch = new LinkedList<DataEvent<V>>();
		}
		
		void terminate()
		{
			synchronized(this)
			{
	            _stop = true;
			   this.notifyAll();
			}
		}
		
		void pauseDataFeed()
		{
		    synchronized(this)
		    {
		        _paused = true;
		    }
		}
		
		void resumeDataFeed()
		{
			synchronized(this)
			{
	            _paused = false;
				this.notifyAll();
			}
		}
		//当从DataProvider中取到了_batchSize个数据 或者从DataProvider取到了null（DataProvider中无数据了）时候，
		//将这些数据flush()到ZoieSystem中
		private void flush()
	    {
	    	// FLUSH
		    Collection<DataEvent<V>> tmp;
		    tmp = _batch;
            _batch = new LinkedList<DataEvent<V>>();

		    try
	        {
		      if(_dataProvider._consumer!=null)
		      {
		    	  _dataProvider._consumer.consume(tmp);
		      }
	        }
	        catch (ZoieException e)
	        {
	          log.error(e.getMessage(), e);
	        }
	    }
		
		public long getCurrentVersion()
		{
			synchronized(this)
			{
		      return _currentVersion;
			}
		}
		
		public void syncWthVersion(long timeInMillis, long version) throws ZoieException
		{
		  long now = System.currentTimeMillis();
		  long due = now + timeInMillis;
		  synchronized(this)
		  {
		    while(_currentVersion < version)
		    {
		      if(now > due)
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
		//消费StreamDataProvider中数据的主要方法
		public void run()
		{
			//可以通过_stop参数来flush数据并停止线程
			while (!_stop)
			{
                //暂停，可以通过StreamDataProvider中相应的方法来暂停该取数据进程的执行 。
				synchronized(this)
                {
				    while(_paused && !_stop)
				    {
				        try {
							this.wait();
					    } catch (InterruptedException e) {
					        continue;
					    }
				    }
                }
				if (!_stop)
				{
					DataEvent<V> data = _dataProvider.next();    //调用_dataProvider.next()取得数据，然后现在自己的list中缓存起来
					//该处会一直取数据，指导达到_dataProvider._batchSize 或者 从_dataProvider取不到数据，则将数据刷新给索引
					//若是达到最大的取数据限制_dataProvider._batchSize，则在刷新完数据给索引后会继续取数据 。
					//若是从_dataProvider中取不到数据，则该线程run()方法结束 。
					if (data!=null)
					{
					  synchronized(this)
					  {
						_batch.add(data);
						if (_batch.size()>=_dataProvider._batchSize)    //达到批量处理数量缓存起来
						{
							flush();
						}
						_currentVersion=Math.max(_currentVersion, data.getVersion());
						this.notifyAll();
					  }
					}
					else
					{
					  synchronized(this)
					  {
						flush();
						_stop=true;
						return;
					  }
					}
				}
			}
		}
	}
}
