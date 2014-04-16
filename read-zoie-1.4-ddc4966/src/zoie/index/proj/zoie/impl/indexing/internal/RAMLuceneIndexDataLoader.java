package proj.zoie.impl.indexing.internal;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Similarity;

public class RAMLuceneIndexDataLoader<R extends IndexReader> extends LuceneIndexDataLoader<R> {

	public RAMLuceneIndexDataLoader(Analyzer analyzer, Similarity similarity,SearchIndexManager<R> idxMgr) {
		super(analyzer, similarity,idxMgr);
	}
	/**
	 * 得到当前可写的内存index 。
	 */
	@Override
	protected BaseSearchIndex getSearchIndex() {
		return _idxMgr.getCurrentWritableMemoryIndex();
	}

}
