package com.elastic.demo;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.logging.Logger;


public class ElasticSearchClient {

    private Client client;
    private boolean connection;
    private static final Logger logger = Logger.getLogger(ElasticSearchClient.class.getName());
    public boolean connectToES(String host, int port) throws UnknownHostException {
        try {
            client = TransportClient.builder().build()
                    .addTransportAddress( new InetSocketTransportAddress(InetAddress.getByName(host), port));
            connection = true;
        } catch (Exception e) {
            logger.info("Error connection"+ e.getMessage());
            connection = false;
        }
        return connection;
    }

    public void closeConnection() {
        try {
            client.close();
            client = null;
        } catch (Exception e) {

        }
    }

    public boolean createIndex(String indexName) {
        Boolean success;
        try {
            client.admin().indices().prepareCreate(indexName).get();
            success = Boolean.TRUE;
        } catch (Exception e){
            success = Boolean.FALSE;
        }
        return success;
    }

    public Boolean deleteIndexByName(String index) {
        Boolean success;
        try {
            client.admin().indices().flush(new FlushRequest(index).force(true)).actionGet();
            DeleteIndexResponse delete = client.admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
            success = !delete.isAcknowledged();
        }
        catch (Exception ex) {
            success = false;
        }
        return success;
    }


    public boolean bulkDocuments(String indexName, String indexType, List<String> jsonDocumentList) {
        Boolean success = Boolean.FALSE;
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        try {
            for (String jsonDocument: jsonDocumentList) {
                bulkRequest.add(client.prepareIndex(indexName, indexType).setSource(jsonDocument));
            }
            BulkResponse bulkResponse = bulkRequest.execute().actionGet();
            if (!bulkResponse.hasFailures()) {
                success = Boolean.TRUE;
            }
        } catch (Exception e){
            success = Boolean.FALSE;
        }
        return success;
    }

    public String getAllProductsQuery(String indexName) {
        String result;
        try {
            SearchResponse response = client.prepareSearch(indexName)
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .execute()
                    .actionGet();
            result = response.toString();
        } catch (Exception e) {
            result = null;
        }
        return result;
    }
}