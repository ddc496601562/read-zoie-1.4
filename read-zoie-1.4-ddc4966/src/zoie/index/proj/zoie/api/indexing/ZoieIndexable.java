package proj.zoie.api.indexing;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;

public interface ZoieIndexable extends Indexable {
	
	/**
	 * 向索引中添加数据的最终的数据类，会把自定义的Indexable构建成IndexingReq，然后添加到索引中
	 * @author cctv
	 *
	 */
	public static final class IndexingReq{
		private final Document _doc;
		private final Analyzer _analyzer;
		
		public IndexingReq(Document doc){
			this(doc,null);
		}
		
		public IndexingReq(Document doc,Analyzer analyzer){
			_doc = doc;
			_analyzer = analyzer;
		}
		
		public Document getDocument(){
			return _doc;
		}
		
		public Analyzer getAnalyzer(){
			return _analyzer;
		}
	}
	
	IndexingReq[] buildIndexingReqs();
}
