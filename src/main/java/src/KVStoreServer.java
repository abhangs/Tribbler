package src;


//This class is used to instantiate a key-value store server

import include.KeyValueStore.KeyValueStore;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;

public class KVStoreServer {

    public static void main(String args[])
    {
        String storageServer = "localhost";
        int storageServerPort = 6379;
        int kvStoreServerPort = 9092;

        try
        {
            KVStoreServerHandler kvStoreServerHandler = new KVStoreServerHandler(storageServer,storageServerPort);

            TProcessor processor = new KeyValueStore.Processor<KeyValueStore.Iface>(kvStoreServerHandler);

            TServerTransport transport = new TServerSocket(kvStoreServerPort);
            TTransportFactory transportFactory = new TTransportFactory();
            TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();

            TSimpleServer simpleServer = new TSimpleServer(new TSimpleServer.Args(transport).processor(processor).inputProtocolFactory(protocolFactory).transportFactory(transportFactory));
            simpleServer.serve();
        }
        catch(Exception ex)
        {

        }
    }
}
