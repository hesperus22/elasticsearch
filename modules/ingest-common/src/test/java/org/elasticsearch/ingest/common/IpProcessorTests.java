package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;

public class IpProcessorTests extends ESTestCase {

    public void test() {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        ingestDocument.setFieldValue("field", "192.168.1.1");
        IpProcessor ipProcessor = new IpProcessor("", "field", false, false, "target_field", null, "ipv4_class", null, null);
        ipProcessor.execute(ingestDocument);
    }
}
