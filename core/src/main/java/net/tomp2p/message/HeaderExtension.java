package net.tomp2p.message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

public final class HeaderExtension {

    private static final Logger LOG = LoggerFactory.getLogger(MessageHeaderCodec.class);

    private String signature;
    private String certificate;
    private int extensionSize;

    public HeaderExtension() {
    }

    public HeaderExtension(String certificate, String signature) {
        this.certificate = certificate;
        this.signature = signature;
    }

    public HeaderExtension(byte[] data, int extensionSize) {
        this.extensionSize = extensionSize;
        decode(data);
    }

    public byte[] encode() {
        byte[] buffer = new byte[MessageHeaderCodec.HEADER_EXTENSION_SIZE];

        String concat = certificate + " " + signature;
        byte[] extensionBytes = concat.getBytes();
        extensionSize = extensionBytes.length;

        System.arraycopy(extensionBytes, 0, buffer, 0, extensionBytes.length);

        LOG.debug("Header Extension encoding size: " + extensionBytes.length);

        return buffer;
    }

    public void decode(byte[] data) {
        byte[] extBytes = Arrays.copyOfRange(data, 0, extensionSize);

        try {
            String[] fullData = new String(data, "UTF-8").split(" ");

            this.certificate = fullData[0];
            this.signature = fullData[1];

        } catch (UnsupportedEncodingException e) {
            LOG.error("Error decoding header extension");
        }
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }

    public String getCertificate() {
        return certificate;
    }

    public void setCertificate(String certificate) {
        this.certificate = certificate;
    }

    public int getExtensionSize() {
        return extensionSize;
    }

    public void setExtensionSize(int extensionSize) {
        this.extensionSize = extensionSize;
    }
}
