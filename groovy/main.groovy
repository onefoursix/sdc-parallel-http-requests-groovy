// The HTTP URL to be called
url = sdc.pipelineParameters()['HTTP_URL']

// The thread pool used to make the parallel requests
executor = sdc.state['executor']

// A wrapper for an HTTPClient that has an "execute" method
// to be called within each thread
HTTPClientWrapper = sdc.state['HTTPClientWrapper']

// An index for each record within a batch
def recordIndexWithinBatch = 0

// Add each record to a list of requests
ArrayList requests = []
for (record in sdc.records) {
    record.attributes['batch_index'] = Integer.toString(sdc.state['batch_index'])
    record.attributes['record_index_within_batch'] = Integer.toString(recordIndexWithinBatch++)
    requests.add(HTTPClientWrapper.execute(url, record, sdc))
}

// Execute all requests and block until they are complete
resultList = executor.invokeAll(requests);

// Handle the records enriched with the HTTP responses
resultList.each {
    for (record in it.get()) {
        try {
          
            httpStatusCode = record.value['httpResponse']['httpStatusCode']

            // Write records with good httpStatusCodes
            if (httpStatusCode == 200) {
                sdc.output.write(record)

            } else {

                // Send records with non-200 httpStatusCodes to error
                sdc.error.write(record, "Received HTTPStatusCode " + httpStatusCode)
            }
        } catch (e) {
            sdc.log.error(e.toString(), e)
            sdc.error.write(record, e.toString())
        }
    }
}

sdc.state['batch_index']++