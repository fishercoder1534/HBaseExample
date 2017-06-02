package com.fishercoder.hbasemr;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;

/**
 * Created by stevesun on 5/11/17.
 */
public class Indexer {
    private static final String SOLR_HOME = "/Users/stevesun/code/playground/hbasemr/solr_home";

    private static void index() throws Exception {
        CoreContainer coreContainer = new CoreContainer(SOLR_HOME);
        coreContainer.load();
        SolrServer server = new EmbeddedSolrServer(coreContainer, "test_core");
        for(int i=0; i<100; ++i) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("cat", "book");
            doc.addField("id", "book-" + i);
            doc.addField("name", "The Legend of the Hobbit part " + i);
            server.add(doc);
        }
        UpdateResponse response = server.commit();
        System.out.println("Response Status code: " + response.getStatus());
        Thread.sleep(1000);
        coreContainer.shutdown();
        server.shutdown();
    }

    private static void serve() throws Exception {
        CoreContainer coreContainer = new CoreContainer(SOLR_HOME);
        coreContainer.load();
        SolrServer server = new EmbeddedSolrServer(coreContainer, "test_core");
        ModifiableSolrParams solrParams = new ModifiableSolrParams();
        solrParams.add(CommonParams.Q, "*:*");
        QueryResponse queryResponse = server.query(solrParams);
        for (SolrDocument document : queryResponse.getResults()) {
            System.out.println(document);
        }
    }

    public static void main(String[] args)  throws Exception {
        index();
        serve();
    }
}
