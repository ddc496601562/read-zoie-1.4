package proj.zoie.impl.indexing.internal;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.impl.util.IntSetAccelerator;
import proj.zoie.api.indexing.ZoieIndexable.IndexingReq;

public abstract class BaseSearchIndex {
	  private static final Logger log = Logger.getLogger(BaseSearchIndex.class);
	  
	  private int _eventsHandled=0;
	  protected MergeScheduler _mergeScheduler;     //合并索引的合并器
	  
	  /**
	   * gets index version, e.g. SCN
	   * @return index version
	   */
	  abstract public long getVersion();
	  
	  /**
	   * gets number of docs in the index, .e.g maxdoc - number of deleted docs
	   * @return
	   */
	  abstract public int getNumdocs();

	  /**
	   * Sets the index version
	   * @param version
	   * @throws IOException
	   */
	  abstract public void setVersion(long version)
	      throws IOException;
	  
	  /**
	   * close and free all resources
	   */
	  abstract public void close();
	  abstract public ZoieIndexReader openIndexReader() throws IOException;
	  
	  /**
	   * 删除的时候打开的IndexReader
	   * @return
	   * @throws IOException
	   */
	  abstract protected IndexReader openIndexReaderForDelete() throws IOException;
	  
	  /**
	   * 更新索引的操作：先删除要添加的文档的uid对应的document，然后再将insertDocs中的添加进去
	   * @param delDocs            需要先删除的uid集合
	   * @param insertDocs         待插入的IndexingReq集合
	   * @param defaultAnalyzer    默认的分词器
	   * @param similarity         
	   * @throws IOException
	   */
	  abstract public IndexWriter openIndexWriter(Analyzer analyzer,Similarity similarity) throws IOException;
	  public void updateIndex(IntSet delDocs, List<IndexingReq> insertDocs,Analyzer defaultAnalyzer,Similarity similarity)
	      throws IOException
	  {
	    deleteDocs(delDocs);
	    IndexWriter idxMod = null;
	    try
	    {
	      idxMod = openIndexWriter(defaultAnalyzer,similarity);
	      if (idxMod != null)
	      { 
	        for (IndexingReq idxPair : insertDocs)
	        {
	          Analyzer analyzer = idxPair.getAnalyzer();
	          Document doc = idxPair.getDocument();
	          if (analyzer == null){
	            idxMod.addDocument(doc);
	          }
	          else{
	        	idxMod.addDocument(doc,analyzer);
	          }
	        }
	      }
	    }
	    finally
	    {
	      if (idxMod!=null)
	      {
	        idxMod.close();
	      }
	    }
	  }
	  /**
	   * 删除delDocs中uid对应的文档
	   * @param delDocs   待删除文档的uid
	   * @throws IOException
	   */
	  private void deleteDocs(IntSet delDocs) throws IOException
	  {
	    int[] delArray=null;
	    if (delDocs!=null && delDocs.size() > 0)
	    {
	      ZoieIndexReader reader= openIndexReader();
	      if (reader!=null)
	      {
	          //存储的是docId和uid的一个对应关系，下标是docId ，值是uid
	    	  int[] uidArray = reader.getUIDArray();
	        
	        IntList delList = new IntArrayList(delDocs.size());
	        for (int i=0;i<uidArray.length;++i)
	        {
	          int uid = uidArray[i];
	          if (uid != ZoieIndexReader.DELETED_UID && delDocs.contains(uid))
	          {
	            delList.add(i);
	          }
	        }
	        delArray = delList.toIntArray();
	      }
	    }
	    
	    if (delArray!=null && delArray.length > 0)
	    {
	      IndexReader readerForDelete = null;
	      try
	      {
	        readerForDelete = openIndexReaderForDelete();
	        if (readerForDelete!=null)
	        {
	          for (int docid : delArray)
	          {
	            readerForDelete.deleteDocument(docid);
	          }
	        }
	      }
	      finally
	      {
	        if (readerForDelete!=null)
	        {
	          try
	          {
	            readerForDelete.close();
	          }
	          catch(IOException ioe)
	          {
	            log.error(ioe.getMessage(),ioe);
	          }
	        }
	      }
	    }
	  }
	  
	  public void loadFromIndex(BaseSearchIndex index) throws IOException
	  {
	    ZoieIndexReader reader = index.openIndexReader();
	    if(reader == null) return;
	    
	    IntSet delSet = new IntSetAccelerator(reader.getModifiedSet());
	    Directory dir = reader.directory();
	    
	    deleteDocs(delSet);
	    
	    IndexWriter writer = null;
	    try
	    {
	      writer = openIndexWriter(null,null);
	      writer.addIndexesNoOptimize(new Directory[] { dir });
	    }
	    finally
	    {
	      if(writer != null) writer.close();
	    }
	  }
	      
	 

	  public void incrementEventCount(int eventCount)
	  {
	    _eventsHandled+=eventCount;
	  }
	  
	  public int getEventsHandled()
	  {
	    return _eventsHandled;
	  }
}
