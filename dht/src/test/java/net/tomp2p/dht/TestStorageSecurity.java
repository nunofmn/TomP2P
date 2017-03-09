package net.tomp2p.dht;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ChannelServerConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.dht.StorageLayer.PutStatus;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.DataMap;
import net.tomp2p.message.KeyMapByte;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.NeighborRPC;
import net.tomp2p.rpc.NeighborRPC.SearchValues;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.*;

public class TestStorageSecurity {
    

    private static String DIR1;

    private static String DIR2;
    
    public static final int PORT_TCP = 5001;
    public static final int PORT_UDP = 5002;

    @Before
    public void before() throws IOException {
        DIR1 = UtilsDHT2.createTempDirectory().getCanonicalPath();
        DIR2 = UtilsDHT2.createTempDirectory().getCanonicalPath();
    }

    @After
    public void after() {
        cleanUp(DIR1);
        cleanUp(DIR2);
    }

    private void cleanUp(String dir) {
        File f = new File(dir);
        f.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                if (pathname.isFile())
                    pathname.delete();
                return false;
            }
        });
        f.delete();
    }


    @Test
    public void testStorePutGetTCP() throws Exception {
        StorageMemory storeSender = new StorageMemory();
        StorageMemory storeRecv = new StorageMemory();
        PeerDHT sender = null;
        PeerDHT recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilderDHT(new PeerBuilder(new Number160("0x50")).p2pId(55).ports(2424).start()).storage(storeSender).start();
            recv1 = new PeerBuilderDHT(new PeerBuilder(new Number160("0x20")).p2pId(55).ports(8088).start()).storage(storeRecv).start();
            StorageRPC smmSender = sender.storeRPC();
            NavigableMap<Number160, Data> tmp = new TreeMap<Number160, Data>();
            byte[] me1 = new byte[] { 1, 2, 3 };
            byte[] me2 = new byte[] { 2, 3, 4 };
            tmp.put(new Number160(77), new Data(me1));
            tmp.put(new Number160(88), new Data(me2));

            FutureChannelCreator fcc = recv1.peer().connectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            PutBuilder putBuilder = new PutBuilder(recv1, new Number160(33));
            putBuilder.domainKey(Number160.createHash("test"));
            DataMap dataMap = new DataMap(new Number160(33), Number160.createHash("test"), Number160.ZERO, tmp);
            putBuilder.dataMapContent(tmp);
            putBuilder.versionKey(Number160.ZERO);

            FutureResponse fr = smmSender.put(recv1.peerAddress(), putBuilder, cc);
            fr.awaitUninterruptibly();
            // get

            GetBuilder getBuilder = new GetBuilder(recv1, new Number160(33));
            getBuilder.domainKey(Number160.createHash("test"));
            getBuilder.contentKeys(tmp.keySet());
            getBuilder.versionKey(Number160.ZERO);

            fr = smmSender.get(recv1.peerAddress(), getBuilder, cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            System.err.println(fr.failedReason());
            Message m = fr.responseMessage();
            Map<Number640, Data> stored = m.dataMap(0).dataMap();
            compare(dataMap.dataMap(), stored);
            System.err.println("done!");
        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
        }
    }


    private void compare(Map<Number640, Data> tmp, Map<Number640, Data> stored) {
        Assert.assertEquals(tmp.size(), stored.size());
        Iterator<Number640> iterator1 = tmp.keySet().iterator();
        while (iterator1.hasNext()) {
            Number640 key1 = iterator1.next();
            Assert.assertEquals(true, stored.containsKey(key1));
            Data data1 = tmp.get(key1);
            Data data2 = stored.get(key1);
            Assert.assertEquals(data1, data2);
        }
    }


    private FutureResponse store(PeerDHT sender, final PeerDHT recv1, StorageRPC smmSender, ChannelCreator cc)
            throws Exception {
        NavigableMap<Number160, Data> tmp = new TreeMap<Number160, Data>();
        byte[] me1 = new byte[] { 1, 2, 3 };
        byte[] me2 = new byte[] { 2, 3, 4 };
        tmp.put(new Number160(77), new Data(me1));
        tmp.put(new Number160(88), new Data(me2));

        AddBuilder addBuilder = new AddBuilder(recv1, new Number160(33));
        addBuilder.domainKey(Number160.createHash("test"));
        addBuilder.dataSet(tmp.values());
        addBuilder.versionKey(Number160.ZERO);

        FutureResponse fr = smmSender.add(recv1.peerAddress(), addBuilder, cc);
        return fr;
    }
}
