package proj.zoie.api;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;

public interface IndexReaderFactory<R extends IndexReader> {
	//得到所有的IndexReader以供搜索
	List<R> getIndexReaders() throws IOException;
	R getDiskIndexReader() throws IOException;
	Analyzer getAnalyzer();
}
