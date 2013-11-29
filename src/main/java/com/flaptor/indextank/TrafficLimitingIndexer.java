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

package com.flaptor.indextank;

import com.flaptor.indextank.index.Document;
import com.flaptor.util.Execute;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * Not yet used or tested yet.
 */
public class TrafficLimitingIndexer implements BoostingIndexer {
    private static final Logger logger = Logger.getLogger(Execute.whoAmI());
    private static final int MAX_NUMBER_OF_PARALLEL_REQUESTS = 3;
    private static final int DEFAULT_MAX_INDEXING_QUEUE_LENGTH = Integer.MAX_VALUE;
    private final BoostingIndexer delegate;
    private final Semaphore semaphore;
    private final int maxIndexingQueueLength;

    public TrafficLimitingIndexer(BoostingIndexer delegate, int maxIndexingQueueLength) {
        Preconditions.checkNotNull(delegate);
        this.delegate = delegate;
        this.maxIndexingQueueLength = maxIndexingQueueLength;
        this.semaphore = new Semaphore(MAX_NUMBER_OF_PARALLEL_REQUESTS, true);
    }

    @Override
    public void dump() throws IOException {
        delegate.dump();
    }

	@Override
	public void del(String docid) {
		this.delegate.del(docid);
	}

    @Override
    public void promoteResult(String docid, String query) {
        this.delegate.promoteResult(docid, query);
    }

	@Override
	public void add(String docId, Document document, int timestampBoost, Map<Integer, Double> dynamicBoosts) { // throws InterruptedException {
        try {
            int queueLen = semaphore.getQueueLength();
            if (queueLen >= maxIndexingQueueLength) {
                logger.warn("Too many waiting to index, queue length = " + queueLen + ", rejecting request");
                throw new InterruptedException("Too many concurrent indexing requests");
            }
            if (queueLen > 0) {
                logger.warn("Concurrent indexing queue length is " + queueLen + ", will wait");
            }
            // consider adding a timeout to this call to semaphore.acquire()
            semaphore.acquire();
            try {
                this.delegate.add(docId, document, timestampBoost, dynamicBoosts);
            } finally {
                semaphore.release();
            }
//        } catch (InterruptedException e) {
        } catch (Exception e) {
            logger.warn("Throwing InterruptedException: " + e.getMessage());
            //throw e;
        }
	}

	@Override
	public void updateBoosts(String docId, Map<Integer, Double> updatedBoosts) {
		this.delegate.updateBoosts(docId, updatedBoosts);		
	}

	@Override
	public void updateTimestamp(String docId, int timestampBoost) {
		this.delegate.updateTimestamp(docId, timestampBoost);		
	}
	
	@Override
	public void updateCategories(String docId, Map<String, String> categories) {
		this.delegate.updateCategories(docId, categories);
	}

    @Override
	public void addScoreFunction(int functionIndex, String definition) throws Exception {
		this.delegate.addScoreFunction(functionIndex, definition);		
	}
    
    @Override
	public void removeScoreFunction(int functionIndex) {
		this.delegate.removeScoreFunction(functionIndex);		
	}
    
    @Override
	public Map<Integer,String> listScoreFunctions() {
		return this.delegate.listScoreFunctions();		
	}

    @Override
    public Map<String, String> getStats() {
        HashMap<String, String> stats = Maps.newHashMap();
        stats.putAll(this.delegate.getStats());
//        stats.putAll(this.storage.getStats());
        return stats;
    }
}
