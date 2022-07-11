// The thread pool used to make the parallel requests
executor = sdc.state['executor']

// A helper to create HTTP Client instances for each record 
HttpClientFactory = sdc.state['HttpClientFactory']

// An index for each record within a batch
def recordIndexWithinBatch = 0

// Add each record to a list of requests
ArrayList requests = []
for (record in sdc.records) {
    record.attributes['batch_index'] = Integer.toString(sdc.state['batch_index'])
    record.attributes['record_index_within_batch'] = Integer.toString(recordIndexWithinBatch++)
    requests.add(HttpClientFactory.createHttpClient(record, sdc))
}

// Execute all requests and block until they are complete
resultList = executor.invokeAll(requests);

// Handle the records enriched with the HTTP responses
resultList.each {
    for (record in it.get()) {

        httpStatusCode = record.value[sdc.userParams['RESPONSE_FIELD']]['httpStatusCode']
        try {
            // Write records with good httpStatusCodes
            if (httpStatusCode == 200) {
                sdc.output.write(record)

            // Send records with non-200 and non-500 httpStatusCodes to error  
            } else {
                sdc.error.write(record, "Received HTTPStatusCode " + httpStatusCode)
            }

        } catch (e) {
            sdc.log.error(e.toString(), e)
            sdc.error.write(record, "Received HTTPStatusCode " + httpStatusCode + " Exceptiom: " + e.toString())
        }

        // Emit event on 500 errors after retries exhausted   
        if (httpStatusCode == 500) {
          sdc.log.info("GROOVY HTTP PROCESSOR: Stoppping pipeline due to server 500 errors with exhausted retries")
          def event = sdc.createEvent("500-Error-with-exhausted-retries", 1)
          sdc.toEvent(event)

        }
    }
}

sdc.state['batch_index']++