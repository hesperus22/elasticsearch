package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;

public class IpProcessorTests extends ESTestCase {

    public void testZoneId() {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        IpProcessor ipProcessor = new IpProcessor("", "field", false, false, "target_field", "zone_id", null, null, null);

        ingestDocument.setFieldValue("field", "192.168.1.1");
        ipProcessor.execute(ingestDocument);
        assertEquals("192.168.1.1", ingestDocument.getFieldValue("target_field", String.class));
        assertFalse(ingestDocument.hasField("zone_id"));

        ingestDocument.setFieldValue("field", "2001:0db8:85a3:0000:0000:8a2e:0370:7334");
        ipProcessor.execute(ingestDocument);
        assertEquals("2001:0db8:85a3:0000:0000:8a2e:0370:7334", ingestDocument.getFieldValue("target_field", String.class));
        assertFalse(ingestDocument.hasField("zone_id"));

        ingestDocument.setFieldValue("field", "2001::8a2e:0370:7334%zone1");
        ipProcessor.execute(ingestDocument);
        assertEquals("2001::8a2e:0370:7334", ingestDocument.getFieldValue("target_field", String.class));
        assertEquals("zone1", ingestDocument.getFieldValue("zone_id", String.class));

        ingestDocument.setFieldValue("field", "192.168.1.1%zone1");
        expectThrows(IllegalArgumentException.class, () -> ipProcessor.execute(ingestDocument));
    }

    public void testIpv4Class() {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        IpProcessor ipProcessor = new IpProcessor("", "field", false, false, null, null, "ip_class", null, null);

        ingestDocument.setFieldValue("field", "2001:0db8:85a3:0000:0000:8a2e:0370:7334");
        ipProcessor.execute(ingestDocument);
        assertFalse(ingestDocument.hasField("ip_class"));

        ingestDocument.setFieldValue("field", "192.168.1.1");
        ipProcessor.execute(ingestDocument);
        assertEquals("D", ingestDocument.getFieldValue("ip_class", String.class));
    }
}
