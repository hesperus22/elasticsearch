package org.elasticsearch.ingest.common;

import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;

import java.net.Inet4Address;
import java.net.InetAddress;

public final class IpProcessor extends AbstractProcessor {

    private static final String TYPE = "ip";
    private final String field;
    private final String targetField;
    private final String zoneIdField;
    private final String ipv4ClassField;
    private final String typeField;
    private final String versionField;
    private final boolean ignoreMissing;
    private final boolean ignoreMalformed;

    IpProcessor(String tag, String field, boolean ignoreMissing, boolean ignoreMalformed, String targetField, String zoneIdField,
                String ipv4ClassField, String typeField, String versionField) {
        super(tag);
        this.field = field;
        this.ignoreMissing = ignoreMissing;
        this.ignoreMalformed = ignoreMalformed;
        this.targetField = targetField;
        this.zoneIdField = zoneIdField;
        this.ipv4ClassField = ipv4ClassField;
        this.typeField = typeField;
        this.versionField = versionField;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        String val = document.getFieldValue(field, String.class, ignoreMissing);

        if (val == null && ignoreMissing) {
            return document;
        } else if (val == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot process it.");
        }

        int zoneSeparator = val.lastIndexOf('%');
        String ipString = val;
        String zoneId = null;
        if (zoneSeparator != -1) {
            ipString = val.substring(0, zoneSeparator);
            zoneId = val.substring(zoneSeparator + 1);
        }

        InetAddress inetAddress;

        try {
            inetAddress = InetAddresses.forString(ipString);
        } catch (IllegalArgumentException e) {
            if (ignoreMalformed) {
                return document;
            }
            throw e;
        }

        if (versionField != null) {
            document.setFieldValue(versionField, inetAddress instanceof Inet4Address ? 4 : 6);
        }

        if (inetAddress instanceof Inet4Address) {
            if (ipv4ClassField != null) {
                byte highByte = inetAddress.getAddress()[0];
                if ((highByte & (1 << 7)) == 0) {
                    document.setFieldValue(ipv4ClassField, "A");
                } else if ((highByte & (1 << 6)) == 0) {
                    document.setFieldValue(ipv4ClassField, "B");
                } else if ((highByte & (1 << 5)) == 0) {
                    document.setFieldValue(ipv4ClassField, "C");
                } else if ((highByte & (1 << 4)) == 0) {
                    document.setFieldValue(ipv4ClassField, "D");
                } else {
                    document.setFieldValue(ipv4ClassField, "E");
                }
            }
        }

        if(typeField!=null){
            if(inetAddress.isMulticastAddress())
        }

        return document;
    }


    @Override
    public String getType() {
        return TYPE;
    }
}
