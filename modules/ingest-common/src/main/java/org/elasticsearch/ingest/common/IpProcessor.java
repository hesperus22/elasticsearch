package org.elasticsearch.ingest.common;

import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public final class IpProcessor extends AbstractProcessor {

    private static final String TYPE = "ip";
    private final boolean ignoreMissing;
    private final boolean ignoreMalformed;
    private final String field;
    private final String targetField;
    private final String zoneIdField;
    private final String ipv4ClassField;
    private final String typeField;
    private final String versionField;
    private final List<String> fields;

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
        fields = Arrays.asList(targetField, zoneIdField, ipv4ClassField, typeField, versionField);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        Object val = document.getFieldValue(field, Object.class, ignoreMissing);

        if (val == null && ignoreMissing) {
            return document;
        } else if (val == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot process it.");
        }

        if (val instanceof String) {
            Map<String, Object> processedIp = processIp((String) val);
            fields.forEach(field -> setFieldValue(document, processedIp, field));
            return document;
        }

        if (val instanceof List == false) {
            throw wrongFieldType();
        }

        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) val;
        List<Map<String, Object>> processedIps = list.stream().map(this::toString).map(this::processIp).collect(Collectors.toList());
        fields.forEach(field -> setFieldValue(document, processedIps, field));

        return document;
    }

    private void setFieldValue(IngestDocument document, Map<String, Object> map, String field) {
        if (field != null && map.get(field) != null) {
            document.setFieldValue(field, map.get(field));
        }
    }

    private void setFieldValue(IngestDocument document, List<Map<String, Object>> list, String field) {
        List<Object> values = list.stream().map(m -> m.get(field)).collect(Collectors.toList());
        if (field != null && values.stream().anyMatch(Objects::nonNull)) {
            document.setFieldValue(field, values);
        }
    }

    private String toString(Object ip) {
        if (ip instanceof String) {
            return (String) ip;
        }
        throw wrongFieldType();
    }

    private Map<String, Object> processIp(String val) {
        int zoneSeparator = val.lastIndexOf('%');
        String ipString = val;
        String zoneId = null;
        if (zoneSeparator != -1) {
            ipString = val.substring(0, zoneSeparator);
            zoneId = val.substring(zoneSeparator + 1);
        }

        Map<String, Object> map = new HashMap<>();
        if (InetAddresses.isInetAddress(ipString) == false) {
            if (ignoreMalformed) {
                return map;
            }
            throw notIpStringLiteral(val);
        }

        InetAddress inetAddress = InetAddresses.forString(ipString);

        map.put(targetField, ipString);
        map.put(versionField, inetAddress instanceof Inet4Address ? 4 : 6);
        map.put(typeField, getType(inetAddress));

        if (inetAddress instanceof Inet6Address) {
            map.put(zoneIdField, zoneId);
            return map;
        }

        if (zoneId != null && ignoreMalformed == false) {
            throw notIpStringLiteral(val);
        }

        map.put(ipv4ClassField, getIpClass(inetAddress));

        return map;
    }

    private String getType(InetAddress inetAddress) {
        if (inetAddress.isMulticastAddress()) {
            return "multicast";
        } else if (inetAddress.isLoopbackAddress()) {
            return "loopback";
        } else if (inetAddress.isLinkLocalAddress()) {
            return "linklocal";
        }
        return "unicast";
    }

    private String getIpClass(InetAddress inetAddress) {
        int highByte = inetAddress.getAddress()[0] + 128;
        if (highByte < 128) {
            return "A";
        } else if (highByte < 192) {
            return "B";
        } else if (highByte < 224) {
            return "C";
        } else if (highByte < 240) {
            return "D";
        }
        return "E";
    }

    private IllegalArgumentException notIpStringLiteral(String val) {
        return new IllegalArgumentException(val + " is not an IP string literal.");
    }

    private IllegalArgumentException wrongFieldType() {
        return new IllegalArgumentException("field [" + field + "] should be string or list of strings");
    }

    public static final class Factory implements Processor.Factory {
        @Override
        public IpProcessor create(Map<String, Processor.Factory> registry, String tag, Map<String, Object> config) {
            String field = ConfigurationUtils.readStringProperty(TYPE, tag, config, "field");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, tag, config, "target_field", field);
            String zoneIdField = ConfigurationUtils.readOptionalStringProperty(TYPE, tag, config, "zone_id_field");
            String ipClassField = ConfigurationUtils.readOptionalStringProperty(TYPE, tag, config, "class_field");
            String typeField = ConfigurationUtils.readOptionalStringProperty(TYPE, tag, config, "type_field");
            String versionField = ConfigurationUtils.readOptionalStringProperty(TYPE, tag, config, "version_field");
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "ignore_missing", false);
            boolean ignoreMalformed = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "ignore_malformed", false);
            return new IpProcessor(tag, field, ignoreMissing, ignoreMalformed, targetField, zoneIdField, ipClassField, typeField,
                versionField);
        }
    }
}
