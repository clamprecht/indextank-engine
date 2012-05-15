package com.flaptor.indextank.index.storage;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

import com.flaptor.indextank.storage.alternatives.DocumentStorage;
import com.flaptor.indextank.storage.alternatives.DocumentStorageFactory;

import com.flaptor.util.Execute;
import com.flaptor.util.FileUtil;
import com.google.common.collect.Maps;
import com.google.common.base.Preconditions;

import krati.core.StoreConfig;
import krati.core.StoreFactory;
//import krati.core.segment.ChannelSegmentFactory;
import krati.core.segment.ChannelSegmentFactory;
import krati.core.segment.SegmentFactory;
import krati.core.segment.WriteBufferSegmentFactory;
import krati.store.DataStore;
import org.apache.log4j.Logger;

/**
 * Stores documents using Krati, a high-performance data store.  See https://github.com/jingwei/krati
 */
public class KratiStorage extends DocumentBinaryStorage {
    private static final Logger logger = Logger.getLogger(Execute.whoAmI());
    private final Charset UTF8_CHARSET;
    private final DataStore<byte[], byte[]> store;

    KratiStorage(File cacheDirectory, String segmentFactoryString, int initialCapacity, int segmentFileSizeMB) throws Exception {
        Preconditions.checkArgument(initialCapacity >= 50000, "Initial capacity must be at least 50000");
        Preconditions.checkArgument(segmentFileSizeMB >= 16, "segmentFileSizeMB must be at least 16");
        Preconditions.checkArgument(segmentFileSizeMB <= 512, "segmentFileSizeMB must be <= 512");
        
        logger.info("Creating a Krati store with initialCapacity " + initialCapacity +
                " and segmentFileSizeMB " + segmentFileSizeMB + ", segmentFactory: " + segmentFactoryString);
        if (!cacheDirectory.exists()) {
            logger.info("Creating new storage directory: " + cacheDirectory.getAbsolutePath());
            cacheDirectory.mkdirs();
        } else {
            logger.info("Removing & creating storage directory: " + cacheDirectory.getAbsolutePath());
            FileUtil.deleteDir(cacheDirectory);
            cacheDirectory.mkdirs();
        }
        this.UTF8_CHARSET = Charset.forName("UTF-8");

        // RAM usage for initialCapacity is initialCapacity * 8 bytes
        StoreConfig config = new StoreConfig(cacheDirectory, initialCapacity);
        config.setBatchSize(10000);     // 10000 performed better loading index
        config.setNumSyncBatches(5);
        final SegmentFactory segmentFactory;
        if ("channel".equals(segmentFactoryString)) {
            // ChannelSegment uses less RAM, with possibly lower write performance
            segmentFactory = new ChannelSegmentFactory();
        } else {    // default
            // WriteBufferSegment may give better write performance, but uses more memory
            // The constructor immediately allocates three ByteBuffers based on segmentFileSizeMB
            segmentFactory = new WriteBufferSegmentFactory(segmentFileSizeMB);
        }
        config.setSegmentFactory(segmentFactory);
        config.setSegmentFileSizeMB(segmentFileSizeMB);
        this.store = StoreFactory.createIndexedDataStore(config);
    }

    @Override
    public byte[] getBinaryDoc(String docId) {
        return this.store.get(docId.getBytes(UTF8_CHARSET));
    }

    @Override
    public void saveBinaryDoc(String docId, byte[] bytes) {
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

    public void close() throws IOException {
        this.store.close();
    }

    @Override
    public Map<String, String> getStats() {
        Map<String, String> stats = Maps.newHashMap();
        stats.put("store_capacity", String.valueOf(store.capacity()));
        return stats;
    }

    public static class Factory implements DocumentStorageFactory {
        /** the KEY for storage directory on the config.  Defaults to 'storage' */
        public static final String DIR = "dir";
        public static final String INITIAL_CAPACITY = "initial_capacity";
        public static final String SEGMENT_FILESIZE_MB = "segment_filesize";
        public static final String SEGMENT_FACTORY = "segment_factory";

        @Override
        public DocumentStorage fromConfiguration(Map<?, ?> config) {
            Preconditions.checkNotNull(config);
            //Preconditions.checkNotNull(config.get(DIR), "config needs '" + DIR + "' value");

            File storageDir = new File("storage");
            if (config.containsKey(DIR)) {
                storageDir = new File(config.get(DIR).toString());
            }
            
            // defaults
            int initialCapacity = 1000000;
            int segmentFileSizeMB = 64;
            String segmentFactory = "writebuffer";

            if (config.containsKey(INITIAL_CAPACITY)) {
                initialCapacity = Integer.valueOf(config.get(INITIAL_CAPACITY).toString());
            }
            if (config.containsKey(SEGMENT_FILESIZE_MB)) {
                segmentFileSizeMB = Integer.valueOf(config.get(SEGMENT_FILESIZE_MB).toString());
            }
            if (config.containsKey(SEGMENT_FACTORY)) {
                segmentFactory = config.get(SEGMENT_FACTORY).toString();
            }

            try {
                return new KratiStorage(storageDir, segmentFactory, initialCapacity, segmentFileSizeMB);
            } catch (Exception e) {
                throw new RuntimeException("while creating a KratiStorage: " + e.getMessage(), e);
            }
        }
    }

}
