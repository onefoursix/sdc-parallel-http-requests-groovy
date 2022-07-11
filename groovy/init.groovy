import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.concurrent.Callable
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.apache.http.Header
import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.HttpClient
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.utils.URIBuilder
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

// A thread pool to execute concurrent HTTP Requests
Executor executor = Executors.newFixedThreadPool(sdc.userParams['MAX_HTTP_PARALLEL_REQUESTS'] as Integer)

// A convenience wrapper used by the main script to create an HTTP Client for each record
class HttpClientFactory {
    static SSLHttpClient createHttpClient(record, sdc) {
        return new SSLHttpClient(record, sdc)
    }
}

// Class that executes an HTTP Request for a given record
class SSLHttpClient implements Callable {

    private static final int connectionTimeoutSeconds = 5;
    private static final int sockerTimeoutSeconds = 5;
    private static final int connectionRequestTimeoutSeconds = 5;

    private record
    private sdc
    private jsonSlurper = new JsonSlurper()

    private boolean retryOn500Response = false
    private int maxBackoffIntervalOn500Response = 0
    private int[] retryIntervals

    // Constructor
    SSLHttpClient(record, sdc) {
        this.record = record
        this.sdc = sdc

        // Is retry on 500 error is enabled, calculate the retry intervals  
        if (sdc.userParams['RETRY_ON_500_SERVER_RESPONSE'].equalsIgnoreCase("true")) {
            retryOn500Response = true
            maxBackoffIntervalOn500Response = sdc.userParams['MAX_BACKOFF_INTERVAL_ON_500_SERVER_ERRORS_SECONDS'] as Integer
            retryIntervals = calculateRetryIntervals(maxBackoffIntervalOn500Response)
        }
    }

    // Calculate the exponential retry intervals based on the max backoff interval    
    // For example, if we have a 300 second maxBackoffSeconds and 10 retries, this method will 
    // return an int array with the values: [1,2,3,5,10,19,38,75,150,300]
    private int[] calculateRetryIntervals(int maxBackoffSeconds) {
      
        // We'll hardcode the number of retries for now
        // though this could be exposed as a parameter
        double numRetries = 10.0
        int[] retries = new int[numRetries];
        double numSlices = 2.0**numRetries
        double sliceLength = maxBackoffSeconds / numSlices
        for (int i = 0; i < numRetries; i++) {
            retries[numRetries - i - 1] = Math.ceil((numSlices / (2**i)) * sliceLength)
        }
        return retries
    }

    // This is the method that executes within each thread
    @Override
    call() {

        // Set connection timeouts
        RequestConfig config = RequestConfig.custom()
            .setConnectTimeout(connectionTimeoutSeconds * 1000)
            .setConnectionRequestTimeout(connectionRequestTimeoutSeconds * 1000)
            .setSocketTimeout(sockerTimeoutSeconds * 1000).build();

        // Init the HTTP Client
        SSLContext sslContext = SSLHttpContextFactory.sslContext();
        URIBuilder builder = new URIBuilder(sdc.userParams['HTTP_URL'])
        HttpClient httpClient = HttpClients.custom().setSSLContext(sslContext).setDefaultRequestConfig(config).build();

        // Create an HTTP POST Request with the specified record "request" field as the payload
        HttpPost request = new HttpPost(builder.build())
        def payload = this.record.value[sdc.userParams['REQUEST_FIELD']]
        def jsonPayload = new JsonBuilder(payload).toString()
        request.setEntity(new StringEntity(jsonPayload));

        // Set Request Properties
        request.setHeader("Accept", "application/json");
        request.setHeader("Content-type", "application/json");
        request.setHeader('Connection', 'keep-alive')

        // Set an Authorization header if needed
        // request.setHeader('Authorization', OMITTED)

        boolean retry = true
        int retryCount = 0
        def serverResponse

        while (retry) {

            // Log the request
            this.sdc.log.info("GROOVY HTTP REQUEST for batch # " + record.attributes['batch_index'] +
                " record # " + record.attributes['record_index_within_batch'])

            // Execute the request      
            HttpResponse response = httpClient.execute(request)

            // Log the response
            this.sdc.log.info("GROOVY HTTP RESPONSE for batch # " + record.attributes['batch_index'] +
                " record # " + record.attributes['record_index_within_batch'])

            // Get the response
            HttpEntity responseEntity = response.getEntity()
            Header encodingHeader = responseEntity.getContentEncoding();
            Charset encoding = encodingHeader == null ? StandardCharsets.UTF_8 :
                Charsets.toCharset(encodingHeader.getValue());
            String serverResponseString = EntityUtils.toString(responseEntity, StandardCharsets.UTF_8);
            serverResponse = jsonSlurper.parseText(serverResponseString)

            // We got a 500 server response, so go through retry logic
            if (serverResponse.httpStatusCode == 500) {
              

                if (!retryOn500Response) {
                    this.sdc.log.info("GROOVY HTTP RESPONSE: 500 Error for batch # " + record.attributes['batch_index'] 
                                  + " record # " + record.attributes['record_index_within_batch'] 
                                  + " Retry on 500 error is not enabled")
                    retry = false
                } else {
                    retryCount++
                    if (retryCount == retryIntervals.size()){
                        break
                    }
                    int sleepInterval = retryIntervals[retryCount] * 1000
                    this.sdc.log.info("GROOVY HTTP RESPONSE: 500 Error for batch # " 
                                      + record.attributes['batch_index'] 
                                      + " record # " + record.attributes['record_index_within_batch'] 
                                      + " -- Retry # " + (retryCount + 1) 
                                      + " will retry in " + retryIntervals[retryCount] + " seconds.")

                    sleep(sleepInterval)
                    retry = true
                }

            // We got a non-500 response so end any retry logic
            } else {
                retry = false
            }
        }

        // Assign the parsed reponse to the specified record "response" field
        this.record.value[sdc.userParams['RESPONSE_FIELD']] = serverResponse

        return this.record
    }
}


// A class that statically creates an SSLContext with a truststore and a keystore
class SSLHttpContextFactory {

    // Set to true if a custom truststore is needed to trust the HTTP Service's certificate
    // If set to false, the default Java cacerts file will be used
    private static boolean CUSTOM_TRUSTSTORE_REQUIRED = false

    // If CUSTOM_TRUSTSTORE_REQUIRED is true, set these variables to point to your custom truststore and truststore password file 
    private static String truststore = "/Users/mark/tls/http_truststore.jks"
    private static String truststorePasswordFile = "/Users/mark/tls/http_truststore_password.txt"

    // Set to true if a keystore is needed for client authentication by the HTTP Service
    private static boolean KEYSTORE_REQUIRED = true

    // If KEYSTORE_REQUIRED is true, set these variables to point to your keystore and keystore password file 
    private static String keystore = "/Users/mark/tls/http_keystore.jks"
    private static String keystorePasswordFile = "/Users/mark/tls/http_keystore_password.txt"

    // The SSLContext used for the life of the pipeline 
    static private final SSLContext sslContext

    // Static initializer for the SSLContext
    static {
        def TrustManager[] trustManagers = CUSTOM_TRUSTSTORE_REQUIRED ? getTrustManagers() : null;
        def KeyManager[] keyManagers = KEYSTORE_REQUIRED ? getKeyManagers() : null;
        sslContext = SSLContext.getInstance("SSL");
        sslContext.init(keyManagers, trustManagers, null);
    }

    static SSLContext sslContext() {
        return sslContext;
    }

    // Get the keystore
    private static KeyManager[] getKeyManagers() throws IOException, GeneralSecurityException {
        String alg = KeyManagerFactory.getDefaultAlgorithm();
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(alg);
        FileInputStream fis = new FileInputStream(keystore);
        KeyStore ks = KeyStore.getInstance("jks");
        String keystorePassword = new File(keystorePasswordFile).getText('UTF-8').trim()
        ks.load(fis, keystorePassword.toCharArray());
        fis.close();
        keyManagerFactory.init(ks, keystorePassword.toCharArray());
        return keyManagerFactory.getKeyManagers();
    }

    // Get the truststore
    private static TrustManager[] getTrustManagers() throws IOException, GeneralSecurityException {
        String alg = TrustManagerFactory.getDefaultAlgorithm();
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(alg);
        FileInputStream fis = new FileInputStream(truststore);
        KeyStore ks = KeyStore.getInstance("jks");
        String truststorePassword = new File(truststorePasswordFile).getText('UTF-8').trim()
        ks.load(fis, truststorePassword.toCharArray());
        fis.close();
        trustManagerFactory.init(ks);
        return trustManagerFactory.getTrustManagers();
    }
}

sdc.state['executor'] = executor
sdc.state['HttpClientFactory'] = HttpClientFactory
sdc.state['batch_index'] = 0