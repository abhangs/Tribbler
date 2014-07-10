package src;


import com.google.gson.Gson;
import include.KeyValueStore.GetListResponse;
import include.KeyValueStore.GetResponse;
import include.KeyValueStore.KVStoreStatus;
import include.KeyValueStore.KeyValueStore;
import include.Tribbler.*;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.TTransport;


import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class TribblerHandler implements Tribbler.Iface, KeyValueStore.Iface {


    //TRIBBLER SERVER: Implements the application logic, each client request results in
    //an RPC call by the Tribbler Server to the key-value storage back-end server and
    //retrieve the required data from the back-end from the data store.

    private String _storageServer;
    private int _storageServerPort;
    private final String UserPrefix = "TribbleUser";
    private final String SubscriptionPrefix = "TribbleSubscription";
    private final String TribblePrefix = "UserTribble";

    private final int MAXRetrievalLimit = 100;


    public TribblerHandler(String storageServer, int storageServerPort)
    {
        // Your initialization goes here
        _storageServer = storageServer;
        _storageServerPort = storageServerPort;

    }


    //Note that there is no interface to delete users. A userid can never be re-used.
    @Override
    public TribbleStatus CreateUser(String userid) throws TException {

        //key = UserPrefix+userid
        //Value = TribbleUser.class in json format

        Gson gson = new Gson();
        GetResponse response = Get(UserPrefix+userid);

        if(response.status!=KVStoreStatus.OK)
        {
            return TribbleStatus.STORE_FAILED;
        }

        TribbleUser tribbleUser =  gson.fromJson(response.getValue(),TribbleUser.class);

        if(tribbleUser.userId.equals(userid))
        {
            return TribbleStatus.EEXISTS;
        }

        TribbleUser newUser = new TribbleUser(userid,new Date());

        String newUserJson = gson.toJson(newUser);

        Put(UserPrefix+userid,newUserJson);

        return TribbleStatus.OK;
    }


    //server should not allow a user to subscribe to a nonexistent user ID, nor allow a nonexistent user ID to subscribe to anyone.
    @Override
    public TribbleStatus AddSubscription(String userid, String subscribeto) throws TException {

        //Gson gson = new Gson();
        GetResponse userResponse = Get(UserPrefix+userid);

        if(userResponse.status!=KVStoreStatus.OK)
        {
            return  TribbleStatus.INVALID_USER;
        }

        GetResponse subscriberResponse = Get(UserPrefix+subscribeto);

        if(subscriberResponse.status!=KVStoreStatus.OK)
        {
            return  TribbleStatus.INVALID_SUBSCRIBETO;
        }

        GetListResponse listResponse = GetList(SubscriptionPrefix+userid);

        if(listResponse.status!=KVStoreStatus.OK)
        {
            if(AddToList(SubscriptionPrefix+userid,UserPrefix+subscribeto)!=KVStoreStatus.OK)
            {
                return  TribbleStatus.STORE_FAILED;
            }

            return TribbleStatus.OK;
        }

        for(String value:listResponse.getValues())
        {
            if(value.equals(UserPrefix + userid))
            {
                return TribbleStatus.EEXISTS;
            }
        }

        if(AddToList(SubscriptionPrefix+userid,UserPrefix+subscribeto)!=KVStoreStatus.OK)
        {
            return  TribbleStatus.STORE_FAILED;
        }

        return TribbleStatus.OK;
    }


    @Override
    public TribbleStatus RemoveSubscription(String userid, String subscribeto) throws TException {
       // Gson gson = new Gson();
        GetResponse userResponse = Get(UserPrefix+userid);

        if(userResponse.status!=KVStoreStatus.OK)
        {
            return  TribbleStatus.INVALID_USER;
        }

        GetResponse subscriberResponse = Get(UserPrefix+subscribeto);

        if(subscriberResponse.status!=KVStoreStatus.OK)
        {
            return  TribbleStatus.INVALID_SUBSCRIBETO;
        }

        GetListResponse listResponse = GetList(SubscriptionPrefix+userid);

        if(listResponse.status!=KVStoreStatus.OK)
        {

            return TribbleStatus.INVALID_SUBSCRIBETO;
        }

        if(RemoveFromList(SubscriptionPrefix+userid,UserPrefix+subscribeto)!=KVStoreStatus.OK)
        {
            return TribbleStatus.STORE_FAILED;
        }

        return TribbleStatus.OK;

    }


    //Client interface to posting a tribble provides only the contents, server is responsible for timestampping the entry and creating a Tribble struct
    //Non-existing user IDs should not be allowed to post or read tribbles
    @Override
    public TribbleStatus PostTribble(String userid, String tribbleContents) throws TException {

        GetResponse userResponse = Get(UserPrefix+userid);
        if(userResponse.status!=KVStoreStatus.OK)
        {
            return TribbleStatus.INVALID_USER;
        }
        Gson gson = new Gson();

        TribbleUser tribbleUser = gson.fromJson(userResponse.getValue(),TribbleUser.class);

        //convert date to long
        long currentTime = new Date().getTime();
        tribbleUser.tribbleDateList.addLast(currentTime);

        Tribble newTribble = new Tribble(userid,currentTime,tribbleContents);

        String key = TribblePrefix+userid+currentTime;

        KVStoreStatus storeStatus = Put(key,gson.toJson(newTribble));

        if(storeStatus!=KVStoreStatus.OK)
        {
            return TribbleStatus.STORE_FAILED;
        }

        return TribbleStatus.OK;
    }

    //Basic function, retrieves a list of most recent tribbles by a particular user,
    //retrieved in reverse chronological order with most recent first, up to 100 max
    @Override
    public TribbleResponse GetTribbles(String userid) throws TException {

        GetResponse userResponse = Get(UserPrefix+userid);
        if(userResponse.status!=KVStoreStatus.OK)
        {
            return new TribbleResponse(null,TribbleStatus.INVALID_USER);
        }

        Gson gson = new Gson();
        TribbleUser tribbleUser = gson.fromJson(userResponse.getValue(),TribbleUser.class);

        int numberOfTribbles = tribbleUser.tribbleDateList.size();

        TribbleResponse tribbleResponse = new TribbleResponse();
        Object tribbleArray[] =  tribbleUser.tribbleDateList.toArray();

        try{

        for(int i=0;i<numberOfTribbles&&i<MAXRetrievalLimit;i++)
        {
           GetResponse getResponse = Get(TribblePrefix+userid+tribbleArray[i]);

           if(getResponse.status!= KVStoreStatus.OK )
           {
               tribbleResponse.status = TribbleStatus.STORE_FAILED;
               return  tribbleResponse;
           }

           Tribble tribble = gson.fromJson(getResponse.getValue(),Tribble.class);
           tribbleResponse.addToTribbles(tribble);
        }

        }catch (Exception ex)
        {
            tribbleResponse.status = TribbleStatus.STORE_FAILED;
            return  tribbleResponse;
        }

        return tribbleResponse;

    }
    //Retrieve a max of 100 most recent tribbles in reverse chronological order
    //from all the users a particular user has subscribed to
    @Override
    public TribbleResponse GetTribblesBySubscription(String userid) throws TException {

        GetResponse userResponse = Get(UserPrefix+userid);
        if(userResponse.status!=KVStoreStatus.OK)
        {
            return new TribbleResponse(null,TribbleStatus.INVALID_USER);
        }

        Gson gson = new Gson();

        GetListResponse subscriptionRetrievalResponse = GetList(SubscriptionPrefix+userid);

        if(subscriptionRetrievalResponse.status!=KVStoreStatus.OK)
        {
            //NOT_IMPLEMENTED means user has not subscribed to any other user
            return new TribbleResponse(null,TribbleStatus.NOT_IMPLEMENTED);
        }

        List<TribbleUser> tribbleUserList = new LinkedList<TribbleUser>();
        TribbleResponse tribbleResponse = new TribbleResponse();

        try{

            for(String key:subscriptionRetrievalResponse.getValues())
            {
               GetResponse getResponse = Get(key);
               if(getResponse.status!=KVStoreStatus.OK)
               {
                   return new TribbleResponse(null,TribbleStatus.STORE_FAILED);
               }

               tribbleUserList.add(gson.fromJson(getResponse.getValue(),TribbleUser.class));
            }

            int i =0;

            for(TribbleUser user:tribbleUserList)
            {
                if(i>99)
                {
                    tribbleResponse.status = TribbleStatus.OK;
                    return tribbleResponse;
                }

                for(Long date:user.tribbleDateList)
                {
                    GetResponse getResponse = Get(TribblePrefix+user.userId+date);
                    if(getResponse.status!=KVStoreStatus.OK)
                    {
                        tribbleResponse.status = TribbleStatus.STORE_FAILED;
                        return tribbleResponse;
                    }

                    tribbleResponse.addToTribbles(gson.fromJson(getResponse.getValue(),Tribble.class));
                    i++;
                }
            }



        }catch (Exception ex)
        {
            return new TribbleResponse(null,TribbleStatus.STORE_FAILED);
        }

        tribbleResponse.status = TribbleStatus.OK;
        return tribbleResponse;
    }

    //function lists the users to whom the target user subscribes
    //make sure you not to report subscriptions for nonexistent userID.
    @Override
    public SubscriptionResponse GetSubscriptions(String userid) throws TException {
        Gson gson = new Gson();
        TribbleUser requestUser =(Get(UserPrefix+userid)).status!=KVStoreStatus.OK?null:
                gson.fromJson((Get(UserPrefix+userid)).value,TribbleUser.class);

        if(requestUser==null)
        {
            return new SubscriptionResponse(null,TribbleStatus.INVALID_USER);
        }

        GetListResponse listResponse = GetList(SubscriptionPrefix+userid);

        if(listResponse.status!=KVStoreStatus.OK)
        {
            //invalid subscribe to means no subscription available for current user
            return new SubscriptionResponse(null,TribbleStatus.INVALID_SUBSCRIBETO);
        }

        SubscriptionResponse response = new SubscriptionResponse(listResponse.getValues(),TribbleStatus.OK);
        return response;

    }



    //Back-end key-value storage server calls.
    //KVStoreStatus is an enumerated data type that denotes the return status of the RPC.

    //GetResponse --> {KVStoreStatus , Values}
    //GetListResponse --> {KVStoreStatus, Values[]}

    //Each RPC request should validate the userid by checking return value of RPC request
    //made to the storage server and return appropriate message to the client.

    //A good implementation will not store a gigantic list of all tribbles for a user in a single key-value entry
    //system should be able to handle users with 1000s of tribbles without excessive bloat or slowdown

    //SUGGESTION: Store a list of tribbles IDs in some way, store each tribble as a separate key-value store item stored on same partition
    // as the user ID.

    // key and value are both of data type strings, need to serialize and de-serialize the stored value
    // using JSON

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
        TSocket socket = new TSocket(_storageServer,_storageServerPort);
        TTransport transport = socket;

        TProtocol protocol = new TBinaryProtocol(transport);
        KeyValueStore.Client client = new KeyValueStore.Client(protocol);

        transport.open();

        GetListResponse response = client.GetList(key);

        transport.close();

        return response;
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
