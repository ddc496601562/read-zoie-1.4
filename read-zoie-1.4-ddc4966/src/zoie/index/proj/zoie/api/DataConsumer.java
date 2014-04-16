package proj.zoie.api;

import java.util.Collection;
import java.util.Comparator;

/**
 * interface for consuming a collection of data events
 * 消费一个DataEvent<V>来进行索引的接口
 * @author jwang
 *
 * @param <V>
 */
public interface DataConsumer<V> {
	
	/**
	 * zoie最终消费的数据都要封装成DataEvent来处理，其中_version作为该data的版本号，会影响这个DataEvent进入索引的先后顺序 。
	 * 同一批data会按照_version来排序后，然后进入索引 。
	 * Data event abstraction
	 * @author jwang
	 * @param <V>
	 */
	public static final class DataEvent<V>
	{
		static Comparator<DataEvent<?>> VERSION_COMPARATOR = new EventVersionComparator();
		private long _version;
		private V _data;
		
		public DataEvent(long version,V data)
		{
			_data=data;
			_version=version;
		}
		
		public long getVersion()
		{
			return _version;
		}
		
		public V getData()
		{
			return _data;
		}
	    
		static public Comparator<DataEvent<?>> getComparator()
		{
		  return VERSION_COMPARATOR;
		}
		
	    static public class EventVersionComparator implements Comparator<DataEvent<?>>
	    {
	      public int compare(DataEvent<?> o1, DataEvent<?> o2)
          {
            if(o1._version < o2._version) return -1;
            else if(o1._version > o2._version) return 1;
            else return 0; 
          }
	      public boolean equals(DataEvent<?> o1, DataEvent<?> o2)
	      {
	        return (o1._version == o2._version);
	      }
	    }
	}
	
	/**
	 * consumption of a collection of data events.
	 * Note that this method may have a side effect. That is it may empty the Collection passed in after execution.
	 * 将Collection<DataEvent<V>> data收集并消费的接口
	 * @param data
	 * @throws ZoieException
	 */
	void consume(Collection<DataEvent<V>> data) throws ZoieException;
}
