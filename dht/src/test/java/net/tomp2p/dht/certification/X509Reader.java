package net.tomp2p.dht.certification;

import net.tomp2p.dht.certification.exception.PrivateKeyReadException;
import net.tomp2p.dht.certification.exception.X509CertificateReadException;
import org.bouncycastle.util.encoders.Base64;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;

import java.io.*;
import java.nio.file.Files;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;

public class X509Reader {

    public X509Certificate readFromFile(String id) throws X509CertificateReadException {

        PemObject pem;

        try {
            PemReader pemReader = new PemReader(new InputStreamReader(new FileInputStream(id + ".cert.pem")));
            pem = pemReader.readPemObject();

            CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
            InputStream in = new ByteArrayInputStream(pem.getContent());

            X509Certificate cert = (X509Certificate)certificateFactory.generateCertificate(in);

            pemReader.close();

            return cert;

        } catch (FileNotFoundException e) {
            throw new X509CertificateReadException("Certificate PEM file not found.");
        } catch (IOException e) {
            throw new X509CertificateReadException("Error reading certificate file.");
        } catch (CertificateException e) {
            throw new X509CertificateReadException("Error reading certificate file.");
        }

    }

    public X509Certificate readFromString(String data) throws X509CertificateReadException {

        try {

            CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");

            InputStream certificate = new ByteArrayInputStream(Base64.decode(data));

            X509Certificate cert = (X509Certificate)certificateFactory.generateCertificate(certificate);

            return cert;

        } catch (CertificateException e) {
            throw new X509CertificateReadException("Error reading certificate from data.");
        }

    }

    public KeyPair readKeyPair(String id) throws X509CertificateReadException, PrivateKeyReadException {

        PublicKey publicKey = readFromFile(id).getPublicKey();
        PrivateKey privateKey = readPrivateKey(id);

        return new KeyPair(publicKey, privateKey);
    }

    public PrivateKey readPrivateKey(String id) throws PrivateKeyReadException {

        PemObject pem;

        try {
            byte[] keyBytes = Files.readAllBytes(new File(id + ".private.der").toPath());

            PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
            KeyFactory kf = KeyFactory.getInstance("RSA");

            return kf.generatePrivate(spec);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
            throw new PrivateKeyReadException("Private Key PEM file not found.");
        } catch (IOException e) {
            e.printStackTrace();
            throw new PrivateKeyReadException("Error reading Private Key Pem file.");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            throw new PrivateKeyReadException("No such algorithm.");
        } catch (InvalidKeySpecException e) {
            e.printStackTrace();
            throw new PrivateKeyReadException("Invalid key spec.");
        }

    }
}
