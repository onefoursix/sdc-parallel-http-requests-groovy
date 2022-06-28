import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.Callable
import org.apache.http.Header
import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.utils.URIBuilder
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

class HTTPClient implements Callable {

    private String url
    private record
    private json_parser
    private sdc

    HTTPClient(String url, record, sdc) {
        this.url = url
        this.json_parser = new JsonSlurper()
        this.record = record
        this.sdc = sdc
    }

    @Override
    call() {

        // Create a Post Request
        URIBuilder builder = new URIBuilder(this.url)
        HttpClient httpClient = HttpClients.createSystem()
        HttpPost request = new HttpPost(builder.build())

        // Set an Authorization header if needed
        // request.setHeader('Authorization', OMITTED)

        // Set the HTTP Post payload.  For this example I will set the entire record.value as
        // the payload, though you may want to set only a specific record.value field
        def payload = this.record.value
        def jsonPayload = new JsonBuilder(payload).toString()
        request.setEntity(new StringEntity(jsonPayload));
      
        // Set Request Properties
        request.setHeader("Accept", "application/json");
        request.setHeader("Content-type", "application/json");
        request.setHeader('Connection', 'keep-alive')

        // Execute the request
        this.sdc.log.info("GROOVY HTTP REQUEST for batch # " + record.attributes['batch_index'] + " record # " + record.attributes['record_index_within_batch'])
        HttpResponse response = httpClient.execute(request)
        this.sdc.log.info("GROOVY HTTP RESPONSE for batch # " + record.attributes['batch_index'] + " record # " + record.attributes['record_index_within_batch'])

        // Get the response
        HttpEntity responseEntity = response.getEntity()
        Header encodingHeader = responseEntity.getContentEncoding();
        Charset encoding = encodingHeader == null ? StandardCharsets.UTF_8 :
            Charsets.toCharset(encodingHeader.getValue());
        String json = EntityUtils.toString(responseEntity, StandardCharsets.UTF_8);
        def jsonSlurper = new JsonSlurper()
      
        // Assign the parsed reponse to new field named 'httpResponse'
        this.record.value['httpResponse'] = jsonSlurper.parseText(json)
      
        return this.record
    }
}

class HTTPClientWrapper {
    static HTTPClient execute(url, record, sdc) {
        return new HTTPClient(url, record, sdc)
    }
}
Executor executor = Executors.newFixedThreadPool(sdc.pipelineParameters()['GROOVY_HTTP_MAX_THREADS'] as Integer)

sdc.state['executor'] = executor
sdc.state['HTTPClientWrapper'] = HTTPClientWrapper
sdc.state['batch_index'] = 0