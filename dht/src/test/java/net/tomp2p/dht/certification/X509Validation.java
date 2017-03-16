package net.tomp2p.dht.certification;

import net.tomp2p.dht.MessageVerifier;
import net.tomp2p.dht.certification.exception.InvalidCertificateException;
import net.tomp2p.dht.certification.exception.X509CertificateReadException;
import net.tomp2p.message.Message;
import net.tomp2p.peers.PeerAddress;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public class X509Validation implements MessageVerifier {

    private final X509Reader certificateReader;
    private X509CertificateVerifier x509Verifier;

    public X509Validation(X509Reader reader) {
        this.certificateReader = reader;
        this.x509Verifier = new X509CertificateVerifier();
    }

    @Override
    public boolean validate(PeerAddress peer, Message message) {

        X509Certificate certificate = null;

        try {
            certificate = certificateReader.readFromString(message.certificate());
            FileInputStream fis = null;

            KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
            fis = new FileInputStream("rethink-ca");
            keystore.load(fis, System.getenv("KEYSTORE_PASS").toCharArray());
            fis.close();

            x509Verifier.verifyCertificate(certificate, keystore);

            //TODO: check peerID against the one in the certificate

            return true;

        } catch (InvalidCertificateException e) {
            System.out.println("Invalid certificate.");
            return false;
        } catch (X509CertificateReadException e) {
            System.out.println("Invalid certificate.");
            return false;
        } catch (CertificateException e) {
            System.out.println("Invalid certificate.");
            return false;
        } catch (NoSuchAlgorithmException e) {
            System.out.println("Invalid certificate.");
            return false;
        } catch (KeyStoreException e) {
            System.out.println("Error loading keystore");
            return false;
        } catch (IOException e) {
            System.out.println("Error loading keystore");
            return false;
        }

    }
}
