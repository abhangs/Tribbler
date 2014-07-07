package src;


import include.KeyValueStore.GetListResponse;
import include.KeyValueStore.GetResponse;
import include.KeyValueStore.KVStoreStatus;
import include.KeyValueStore.KeyValueStore;
import include.Tribbler.SubscriptionResponse;
import include.Tribbler.TribbleResponse;
import include.Tribbler.TribbleStatus;
import include.Tribbler.Tribbler;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TFileTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class TribblerHandler implements Tribbler.Iface, KeyValueStore.Iface {

    private String _storageServer;
    private int _storageServerPort;

    public TribblerHandler(String storageServer, int storageServerPort)
    {
        // Your initialization goes here
        _storageServer = storageServer;
        _storageServerPort = storageServerPort;

    }

    @Override
    public TribbleStatus CreateUser(String userid) throws TException {
        return TribbleStatus.NOT_IMPLEMENTED;
    }

    @Override
    public TribbleStatus AddSubscription(String userid, String subscribeto) throws TException {
        return TribbleStatus.NOT_IMPLEMENTED;
    }

    @Override
    public TribbleStatus RemoveSubscription(String userid, String subscribeto) throws TException {
        return null;
    }

    @Override
    public TribbleStatus PostTribble(String userid, String tribbleContents) throws TException {
        return null;
    }

    @Override
    public TribbleResponse GetTribbles(String userid) throws TException {
        return null;
    }

    @Override
    public TribbleResponse GetTribblesBySubscription(String userid) throws TException {
        return null;
    }

    @Override
    public SubscriptionResponse GetSubscriptions(String userid) throws TException {
        return null;
    }


    @Override
    public GetResponse Get(String key) throws TException {
       TSocket socket = new TSocket(_storageServer,_storageServerPort);
       TTransport transport = socket;

       TProtocol protocol = new TBinaryProtocol(transport);
       KeyValueStore.Client client = new KeyValueStore.Client(protocol);

       transport.open();

       GetResponse response = client.Get(key);

       transport.close();

       return response;
    }

    @Override
    public GetListResponse GetList(String key) throws TException {
        return null;
    }

    @Override
    public KVStoreStatus Put(String key, String value) throws TException {
        TSocket socket = new TSocket(_storageServer,_storageServerPort);
        TTransport transport = socket;
        TProtocol protocol = new TBinaryProtocol(transport);

        KeyValueStore.Client client = new KeyValueStore.Client(protocol);

        KVStoreStatus storeStatus;

        transport.open();

        storeStatus = client.Put(key,value);

        transport.close();

        return storeStatus;
    }

    @Override
    public KVStoreStatus AddToList(String key, String value) throws TException {

        TSocket socket = new TSocket(_storageServer,_storageServerPort);
        TTransport transport = socket;
        TProtocol protocol = new TBinaryProtocol(transport);

        KeyValueStore.Client client = new KeyValueStore.Client(protocol);

        KVStoreStatus storeStatus ;

        transport.open();

        storeStatus = client.AddToList(key,value);

        transport.close();

        return storeStatus;



    }

    @Override
    public KVStoreStatus RemoveFromList(String key, String value) throws TException {
        TSocket socket = new TSocket(_storageServer,_storageServerPort);
        TTransport transport = socket;
        TProtocol protocol = new TBinaryProtocol(transport);
        KeyValueStore.Client client = new KeyValueStore.Client(protocol);
        KVStoreStatus storeStatus;

        transport.open();

        storeStatus = client.RemoveFromList(key,value);

        transport.close();

        return  storeStatus;
    }
}
