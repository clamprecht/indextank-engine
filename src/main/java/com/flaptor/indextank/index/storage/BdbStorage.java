/*
 * Copyright (c) 2011 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.flaptor.indextank.index.storage;

import com.flaptor.indextank.index.Document;
import com.flaptor.indextank.storage.alternatives.DocumentStorage;
import com.flaptor.indextank.storage.alternatives.DocumentStorageFactory;
import com.flaptor.util.Execute;
import com.flaptor.util.FileUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.sleepycat.db.*;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Berkeley DB storage implementation.
 * @author clamprecht
 */
public class BdbStorage extends DocumentBinaryStorage {
    private static final Logger logger = Logger.getLogger(Execute.whoAmI());
    private static final String MAIN_FILE_NAME = "BdbStorage";
    private final Charset UTF8_CHARSET = Charset.forName("UTF-8");
    private final Database database;
    //private final File storageDir;

    public BdbStorage(File storageDir, int cacheSizeMB, DatabaseConfig config) throws IOException {
        Preconditions.checkArgument(cacheSizeMB >= 1, "cacheSizeMB must be at least 1");
        Preconditions.checkArgument(cacheSizeMB <= 4096, "cacheSizeMB must be <= 4096");
        Preconditions.checkNotNull(storageDir);
        //this.storageDir = storageDir;

        //File storageFile = new File(storageDir, MAIN_FILE_NAME);
        logger.info("Creating a BDB store with cache size " + cacheSizeMB + " MB in " + storageDir);
        if (config.getType() == DatabaseType.BTREE) {
            logger.info("BDB storage type: BTREE");
        } else if (config.getType() == DatabaseType.HASH) {
            logger.info("BDB storage type: HASH, fillfactor " + config.getHashFillFactor() +
                ", numElements " + config.getHashNumElements());
        }

        if (!storageDir.exists()) {
            logger.info("Creating new storage directory: " + storageDir.getAbsolutePath());
            storageDir.mkdirs();
        } else {
            logger.info("Removing & creating storage directory: " + storageDir.getAbsolutePath());
            FileUtil.deleteDir(storageDir);
            storageDir.mkdirs();
        }
        try {
            // Setup BDB environment using Concurrent Data Store
            EnvironmentConfig ec = new EnvironmentConfig();
            ec.setAllowCreate(true);
            ec.setInitializeCDB(true);
            ec.setCacheSize(cacheSizeMB * 1024 * 1024);
            ec.setInitializeCache(true);
            ec.setErrorStream(getErrorStream());
            ec.setErrorPrefix("BDBError");
            Environment env = new Environment(storageDir, ec);

//            DatabaseConfig config = new DatabaseConfig();
            //config.setErrorStream(getErrorStream());
            //config.setErrorPrefix("BDBError");
            // HASH may be better for large datasets with no key locality.
//            config.setType(dbType); // BTREE or HASH
            config.setAllowCreate(true);
            config.setCacheSize(cacheSizeMB * 1024 * 1024);

            //config.set
            /* you want to select a page size that is at least large enough to hold multiple entries
            given the expected average size of your database entries. In BTree's case, for best results
            select a page size that can hold at least 4 such entries. */
            // must be power of 2
            //config.setPageSize()
            //database = new Database(storageFile.getAbsolutePath(), null, config);
            database = env.openDatabase(null, MAIN_FILE_NAME, null, config);
        } catch (DatabaseException e) {
            logger.error("DatabaseException in BdbStorage", e);
            throw new IOException(e);
        }
    }

    public PrintStream getErrorStream() {
        return System.err;
    }

    public void dump() throws IOException {
        syncToDisk();
    }

    public void close() throws IOException {
        try {
            database.close();
        } catch (DatabaseException e) {
            throw new IOException(e);
        }
    }

    /**
     * Serializes this instance content to disk.
     * Blocking method.
     */
    private synchronized void syncToDisk() throws IOException {
        logger.info("Syncing to disk.");
        try {
            database.sync();
            database.close();
            logger.info("Sync to disk completed.");
        } catch (DatabaseException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected byte[] getBinaryDoc(String docId) {
        try {
            DatabaseEntry key = new DatabaseEntry(docId.getBytes(UTF8_CHARSET));
            DatabaseEntry readValue = new DatabaseEntry();
            OperationStatus status = database.get(null, key, readValue, LockMode.DEFAULT);
            // see http://docs.oracle.com/cd/E17076_02/html/programmer_reference/am_misc_bulk.html
//            MultipleKeyDataEntry mkey = new MultipleKeyDataEntry();
            if (status == OperationStatus.SUCCESS) {
                return readValue.getData();
            }
            if (status != OperationStatus.NOTFOUND) {
                logger.error("Error reading doc from BDB, status: " + status);
            }
            return null;
        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void saveBinaryDoc(String docId, byte[] bytes) {
        try {
            DatabaseEntry key = new DatabaseEntry(docId.getBytes(UTF8_CHARSET));
            DatabaseEntry value = new DatabaseEntry(bytes);
            OperationStatus status = database.put(null, key, value);
            if (status != OperationStatus.SUCCESS) {
                logger.error("Error saving doc to BDB, status: " + status);
            }
        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteBinaryDoc(String docId) {
        try {
            DatabaseEntry key = new DatabaseEntry(docId.getBytes(UTF8_CHARSET));
            OperationStatus status = database.delete(null, key);
            if (status != OperationStatus.SUCCESS && status != OperationStatus.NOTFOUND) {
                logger.error("Error deleting doc from BDB, status: " + status);
            }
        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, String> getStats() {
        HashMap<String, String> stats = Maps.newHashMap();
        try {
            // get key length stats from subclass
            stats.putAll(getLengthStats());
            StatsConfig statsConfig = new StatsConfig();
            statsConfig.setFast(true);
            DatabaseStats bdbStats = database.getStats(null, statsConfig);
            if (bdbStats != null) {
                stats.put("bdb_stats", bdbStats.toString());
            }
            //stats.put("in_memory_storage_count", String.valueOf(compressedMap.size()));
            return stats;
        } catch (DatabaseException e) {
            stats.put("storage_stats", "error");
            return stats;
        }
    }

    public static class Factory implements DocumentStorageFactory {
        /** the KEY for storage directory on the config.  Defaults to 'storage' */
        public static final String DIR = "dir";
        public static final String BDB_CACHE_MB = "bdb_cache";
        public static final String BDB_TYPE = "bdb_type";
        public static final String HASH_FILLFACTOR = "hashff";
        public static final String HASH_NUMELEMENTS = "hashsize";

        @Override
        public DocumentStorage fromConfiguration(Map<?, ?> config) {
            Preconditions.checkNotNull(config);
            //Preconditions.checkNotNull(config.get(DIR), "config needs '" + DIR + "' value");

            File storageDir = new File("storage");
            if (config.containsKey(DIR)) {
                storageDir = new File(config.get(DIR).toString());
            }

            // defaults
            int cacheSizeMB = 10;
            if (config.containsKey(BDB_CACHE_MB)) {
                cacheSizeMB = Integer.valueOf(config.get(BDB_CACHE_MB).toString());
            }

            DatabaseType dbType = DatabaseType.BTREE;
            if (config.containsKey(BDB_TYPE)) {
                String type = (String) config.get(BDB_TYPE);
                if ("btree".equalsIgnoreCase(type)) {
                    dbType = DatabaseType.BTREE;
                } else if ("hash".equalsIgnoreCase(type)) {
                    dbType = DatabaseType.HASH;
                } else {
                    throw new IllegalArgumentException("Invalid bdb_type: " + type);
                }
            }

            DatabaseConfig databaseConfig = new DatabaseConfig();
            //databaseConfig.setErrorStream(getErrorStream());
            //databaseConfig.setErrorPrefix("BDBError");
            // HASH may be better for large datasets with no key locality.
            databaseConfig.setType(dbType);
            databaseConfig.setAllowCreate(true);
            databaseConfig.setCacheSize(cacheSizeMB * 1024 * 1024);
            if (dbType.equals(DatabaseType.HASH)) {
                int hashFillFactor = -1;
                int hashNumElements = -1;
                if (config.containsKey(HASH_FILLFACTOR)) {
                    hashFillFactor = Integer.parseInt((String) config.get(HASH_FILLFACTOR));
                }
                if (config.containsKey(HASH_NUMELEMENTS)) {
                    hashNumElements = Integer.parseInt((String) config.get(HASH_NUMELEMENTS));
                }

                // HASH config:
                // see http://docs.oracle.com/cd/E17076_02/html/programmer_reference/hash_conf.html
                // A reasonable rule computing fill factor is to set it to the following:
                // (pagesize - 32) / (average_key_size + average_data_size + 8)
                // (4096 - 32) / (50 + 1000 + 8) = 3.84120983
                // maybe we should record stats on avg key size and data size
                if (hashFillFactor > 0 && hashNumElements > 0) {
                    databaseConfig.setHashFillFactor(hashFillFactor);
                    databaseConfig.setHashNumElements(hashNumElements);
                }
            }

            try {
                return new BdbStorage(storageDir, cacheSizeMB, databaseConfig);
            } catch (Exception e) {
                throw new RuntimeException("while creating a BdbStorage: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Allows testing changes to the compression method, it first
     * validates the correctness of the implementation and then
     * lists the compression value and ratio for several document
     * sizes.
     * <p/>
     * First argument should be the text to use for texting, it will
     * be clipped to different sizes for ratio testing.
     */
    public static void main(String[] args) throws IOException {
        //testCorrectness(args);
        //testCompressionRatio(args);
        int cacheSizeMB = 10;
        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setType(DatabaseType.BTREE);
        databaseConfig.setAllowCreate(true);
        databaseConfig.setCacheSize(cacheSizeMB * 1024 * 1024);
        BdbStorage ims = new BdbStorage(new File(args[0]), cacheSizeMB, databaseConfig);
        Scanner in = new Scanner(System.in);

        while (in.hasNextLine()) {
            Document document = ims.getDocument(in.nextLine());
            System.out.println(document);
        }

    }

    private static void testCorrectness(String[] args) throws IOException {
        File storageDir = FileUtil.createTempDir("testBdbStorage", ".tmp");
        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setType(DatabaseType.BTREE);
        databaseConfig.setAllowCreate(true);
        int cacheSizeMB = 10;
        databaseConfig.setCacheSize(cacheSizeMB * 1024 * 1024);
        BdbStorage storage = new BdbStorage(storageDir, cacheSizeMB, databaseConfig);
        Document doc1 = new Document();
        doc1.setField("text", args[0]);
        storage.saveDocument("a", doc1);
        Document dd1 = storage.getDocument("a");
        Preconditions.checkState(dd1.equals(doc1), dd1 + " - " + doc1);
        Document doc2 = new Document();
        doc2.setField("nottext", args[0]);
        storage.saveDocument("b", doc2);
        Document dd2 = storage.getDocument("b");
        Preconditions.checkState(dd2.equals(doc2), dd2);
        Document doc3 = new Document();
        doc3.setField("text", args[0]);
        doc3.setField("f1", "v1");
        doc3.setField("f2", "v2");
        storage.saveDocument("c", doc3);
        Document dd3 = storage.getDocument("c");
        Preconditions.checkState(dd3.equals(doc3), dd3);
    }
}
