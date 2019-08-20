package com.bigdata.hbase;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;

import com.bigdata.es.ESClient;

/**
 * Bulk hbase data to ElasticSearch Class
 */
public class ElasticSearchBulkOperator {

    private static final int MAX_BULK_COUNT = 1000;

    private static BulkRequestBuilder bulkRequestBuilder = null;

    private static final Lock commitLock = new ReentrantLock();

    private static ScheduledExecutorService scheduledExecutorService = null;
    
    private static Logger logger = LogManager.getLogger(HbaseDataSyncEsObserver.class);

    static {
        // init es bulkRequestBuilder
        bulkRequestBuilder = ESClient.getEsClient().prepareBulk();
       // bulkRequestBuilder.setRefresh(true);

        // init thread pool and set size 1
        scheduledExecutorService = Executors.newScheduledThreadPool(1);

        // create beeper thread( it will be sync data to ES cluster)
        // use a commitLock to protected bulk es as thread-save
        final Runnable beeper = new Runnable() {
            public void run() {
                commitLock.lock();
                try {
                    bulkRequest(0);
                } catch (Exception ex) {
                    System.out.println(ex.getMessage());
                   // LOG.error("Time Bulk " + ESClient.indexName + " index error : " + ex.getMessage());
                } finally {
                    commitLock.unlock();
                }
            }
        };

        // set time bulk task
        // set beeper thread(10 second to delay first execution , 30 second period between successive executions)
        scheduledExecutorService.scheduleAtFixedRate(beeper, 10, 30, TimeUnit.SECONDS);

    }

    /**
     * shutdown time task immediately
     */
    public static void shutdownScheduEx() {
        if (null != scheduledExecutorService && !scheduledExecutorService.isShutdown()) {
            scheduledExecutorService.shutdown();
        }
    }

    /**
     * bulk request when number of builders is grate then threshold
     *
     * @param threshold
     */
    public static void bulkRequest(int threshold) {
    	int actions = bulkRequestBuilder.numberOfActions();
        if (actions > threshold) {
            BulkResponse bulkItemResponse = bulkRequestBuilder.execute().actionGet();
            if (!bulkItemResponse.hasFailures()) {
            	logger.info("------------update es doc success, actions=="+actions);
                bulkRequestBuilder = ESClient.getEsClient().prepareBulk();
            }else{
            	logger.info("------------update es doc fail!!!!!!, actions=="+actions +"; error is:"+bulkItemResponse.buildFailureMessage());
            }
        }
    }

    /**
     * add update builder to bulk
     * use commitLock to protected bulk as thread-save
     * @param builder
     */
    public static void addUpdateBuilderToBulk(UpdateRequestBuilder builder) {
        commitLock.lock();
        try {
            bulkRequestBuilder.add(builder);
            bulkRequest(MAX_BULK_COUNT);
        } catch (Exception ex) {
            //LOG.error(" update Bulk " + ESClient.indexName + " index error : " + ex.getMessage());
        } finally {
            commitLock.unlock();
        }
    }

    /**
     * add delete builder to bulk
     * use commitLock to protected bulk as thread-save
     *
     * @param builder
     */
    public static void addDeleteBuilderToBulk(DeleteRequestBuilder builder) {
        commitLock.lock();
        try {
            bulkRequestBuilder.add(builder);
            bulkRequest(MAX_BULK_COUNT);
        } catch (Exception ex) {
           // LOG.error(" delete Bulk " + ESClient.indexName + " index error : " + ex.getMessage());
        } finally {
            commitLock.unlock();
        }
    }
}
