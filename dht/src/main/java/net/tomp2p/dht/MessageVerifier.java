package net.tomp2p.dht;

import net.tomp2p.message.Message;

public interface MessageVerifier {

    public boolean validate(Message message);
}
