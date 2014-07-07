package src;


import include.Tribbler.Tribbler;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

public class TribblerServer {

    public static void main(String args[])
    {
//        if(args.length!=4)
//        {
//            return;
//        }

        /*String storageServer = args[1];
        int storageServerPort = Integer.parseInt(args[2]);
        int tribblerPort = Integer.parseInt(args[3]);
        */

        //hard coding values

        String storageServer = "sysnet89.sysnet.ucsd.edu";
        int storageServerPort = 7070;
        int tribblerPort = 9090;


        TribblerHandler tribblerHandler = new TribblerHandler(storageServer,storageServerPort);
        TProcessor processor = new Tribbler.Processor<Tribbler.Iface>(tribblerHandler);
        try {

            TServerTransport serverTransport = new TServerSocket(tribblerPort);
            TTransportFactory transportFactory = new TTransportFactory();
            TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();

            TSimpleServer server = new TSimpleServer(new TSimpleServer.Args(serverTransport).processor(processor).inputProtocolFactory(protocolFactory).transportFactory(transportFactory));
            server.serve();


        }

        catch (Exception ex)
        {

        }




    }

}
