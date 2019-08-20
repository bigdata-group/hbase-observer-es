package com.bigdata.hbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.bigdata.es.ESClient;

/**
 * Hbase Sync data to Es Class
 */
public class HbaseDataSyncEsObserver extends BaseRegionObserver {
	private Logger logger = LogManager.getLogger(HbaseDataSyncEsObserver.class);
   
    private String index;
    private String type;
    private List<String> fields;
	
    /**
     * 对应创建observer时候的参数
     * @param env
     */
    public void readConfiguration(CoprocessorEnvironment env) {
        Configuration conf = env.getConfiguration();
        this.index = conf.get("es_index");
        this.type = conf.get("es_type");
        Properties props = ESClient.readProperties("app.properties");
        String es_fields = props.getProperty(this.type+"_fields");
        fields = Arrays.asList(es_fields.split(","));
    }

    /**
     *  start
     * @param e
     * @throws IOException
     */
    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
    	 // read config
        readConfiguration(e);
        // init ES client
    	ESClient.getEsClient();
    	logger.info("------observer init EsClient ------");
    }

    /**
     * stop
     * @param e
     * @throws IOException
     */
    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
       // flush
       ElasticSearchBulkOperator.bulkRequest(0);
       // close es client
       ESClient.closeEsClient();
       // shutdown time task
       ElasticSearchBulkOperator.shutdownScheduEx();
       
       logger.info("------observer stop ------");
    }

    /**
     * Called after the client stores a value
     * after data put to hbase then prepare update builder to bulk  ES
     *
     * @param e
     * @param put
     * @param edit
     * @param durability
     * @throws IOException
     */
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        String indexId = new String(put.getRow());
        if (indexId.indexOf("_AYX") > 0){
	        try {
	            NavigableMap<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
	            Map<String, Object> json = new HashMap<String, Object>();
	            //json.put("rowkey", indexId);
	            for (Map.Entry<byte[], List<Cell>> entry : familyMap.entrySet()) {
	                for (Cell cell : entry.getValue()) {
	                    String key = Bytes.toString(CellUtil.cloneQualifier(cell));
	                    if (fields.contains(key)){
	                    	String value = Bytes.toString(CellUtil.cloneValue(cell));
	                    	logger.info("key===" +key + ";value===" + value);
	                    	json.put(key, value);
	                    }
	                }
	            }
	            // set hbase family to es
	            if (json.size() > 0){
	            	ElasticSearchBulkOperator.addUpdateBuilderToBulk(ESClient.getEsClient().prepareUpdate(index, type, indexId).setDocAsUpsert(true).setDoc(json));
	            }
	        } catch (Exception ex) {
	        	logger.error(ex);
	        }
        }
    }


    /**
     * Called after the client deletes a value.
     * after data delete from hbase then prepare delete builder to bulk  ES
     * @param e
     * @param delete
     * @param edit
     * @param durability
     * @throws IOException
     */
    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
	    String indexId = new String(delete.getRow());
	    if (indexId.indexOf("_AYX") > 0){
	        try {
	            ElasticSearchBulkOperator.addDeleteBuilderToBulk(ESClient.getEsClient().prepareDelete(index, type, indexId));
	        } catch (Exception ex) {
	        	logger.error(ex);
	        }
    	}
    }
}
