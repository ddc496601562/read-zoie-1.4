package proj.zoie.api.indexing;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;

import proj.zoie.api.ZoieIndexReader;
/**
 * 将ZoieIndexReader进行包装的装饰类
 * @author cctv
 *
 * @param <R>
 */
public interface IndexReaderDecorator<R extends IndexReader>
{
	R decorate(ZoieIndexReader indexReader) throws IOException;
}
