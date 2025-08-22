
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

public class ElasticsearchIndexer {
    public static void main(String[] args) throws IOException {
        // Connect to Elasticsearch
        RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200)).build();
        ElasticsearchClient client = new ElasticsearchClient(new RestClientTransport(restClient, new JacksonJsonpMapper()));

        // Directory containing JSON files
        File jsonDir = new File("json_output");
        ObjectMapper mapper = new ObjectMapper();

        for (File file : jsonDir.listFiles()) {
            if (file.getName().endsWith(".json")) {
                String indexName = file.getName().replace(".json", "");
                List<Map<String, Object>> documents = mapper.readValue(file, new TypeReference<List<Map<String, Object>>>() {});

                for (Map<String, Object> doc : documents) {
                    IndexRequest<Map<String, Object>> request = IndexRequest.of(i -> i
                        .index(indexName)
                        .document(doc)
                    );
                    client.index(request);
                }
                System.out.println("Indexed " + documents.size() + " documents into '" + indexName + "' index.");
            }
        }
    }
}
