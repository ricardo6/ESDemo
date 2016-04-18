package com.elastic.demo;

import org.elasticsearch.client.Client;
import java.io.IOException;


public class ESMain {
    private static Client client;

    public static void main(String args []) throws IOException {
        String indexName = "products";
        String indexTypeName = "product";


        System.out.println("Connecting to elasticsearch... ");
        ElasticSearchClient ESClient = new ElasticSearchClient();
        DocumentReader reader = new DocumentReader();

        ESClient.connectToES("localhost", 9300);
        System.out.println("Creating a new index products... ");

        ESClient.createIndex(indexName);
        System.out.println("The new index 'products' was created successfully.");

        boolean documentAdded = ESClient.bulkDocuments(
                indexName, indexTypeName, reader.getDocumentsFromFile("test-data.json"));

        if (documentAdded) {
            System.out.println("The sample product documents was added successfully.");

            System.out.println("Searching all products from index... ");

            String result = ESClient.getAllProductsQuery(indexName);
            System.out.printf("result: %s", result);
        }

        System.out.println("Deleting index product ....");
        if (ESClient.deleteIndexByName(indexName)) {
            System.out.println("Index products was deleted successfully.");
        }
        ESClient.closeConnection();
    }
}