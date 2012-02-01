package com.flaptor.indextank.index.storage;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

import com.flaptor.indextank.index.Document;
import com.flaptor.indextank.storage.alternatives.DocumentStorage;

import com.google.common.collect.Maps;

import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.core.segment.MemorySegmentFactory;
import krati.store.DataStore;

public class KratiStorage extends DocumentBinaryStorage {

  private File cacheDirectory;
  private Charset UTF8_CHARSET ;
  private DataStore<byte[], byte[]> store;

  public KratiStorage(File cacheDirectory) throws Exception {
    this.cacheDirectory = cacheDirectory;

    StoreConfig config = new StoreConfig(cacheDirectory, 1024);
    config.setSegmentFactory(new MemorySegmentFactory());
    config.setSegmentFileSizeMB(64);
        
    this.store = StoreFactory.createStaticDataStore(config);
    this.UTF8_CHARSET = Charset.forName("UTF-8");
  }

  @Override
  public byte[] getBinaryDoc(String docId) {
    return this.store.get(docId.getBytes(UTF8_CHARSET));
  }

  @Override
  public void saveBinaryDoc(String docId, byte[] bytes){
    try { 
      this.store.put(docId.getBytes(UTF8_CHARSET), bytes);
    } catch (Exception e) {
      throw new RuntimeException("while savingBinaryDoc:" + e.getMessage(), e);
    }
  }

  @Override
  public void deleteBinaryDoc(String docId) {
    try { 
      this.store.delete(docId.getBytes(UTF8_CHARSET));
    } catch (Exception e) {
      throw new RuntimeException("while deleting BinaryDoc:" + e.getMessage(), e);
    }
  }

  @Override
  public void dump() throws IOException {
    this.store.persist();
  }

  @Override
  public Map<String, String> getStats() {
    return Maps.newHashMap();
  }

}
