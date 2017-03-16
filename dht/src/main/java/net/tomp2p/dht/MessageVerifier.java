package net.tomp2p.dht;

import net.tomp2p.message.Message;
import net.tomp2p.peers.PeerAddress;

public interface MessageVerifier {

    public boolean validate(PeerAddress peer, Message message);
}
