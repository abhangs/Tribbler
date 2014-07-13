package src;

import include.KeyValueStore.GetListResponse;
import include.KeyValueStore.GetResponse;
import include.KeyValueStore.KVStoreStatus;
import include.KeyValueStore.KeyValueStore;
import org.apache.thrift.TException;
import redis.clients.jedis.Jedis;
import sun.awt.CausedFocusEvent;
import sun.org.mozilla.javascript.internal.ast.NewExpression;

import javax.tools.JavaCompiler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.concurrent.ExecutionException;

//This class implements the key-value backend server
//REDIS (open source) key-value store used for persistent storage
//JEDIS: Using open source JEDIS as java client for REDIS

public class KVStoreServerHandler implements KeyValueStore.Iface {

    private String _storageServer;
    private int _storageServerPort;

    private static Jedis jedis;
    private static GetResponse response;
    private static GetListResponse listResponse;

    public KVStoreServerHandler(String storageServer, int storageServerPort)
    {
        _storageServer = storageServer;
        _storageServerPort = storageServerPort;

        jedis = new Jedis(_storageServer,_storageServerPort);

        response = new GetResponse();
        listResponse = new GetListResponse();

    }


    @Override
    public GetResponse Get(String key) throws TException {
        try
        {
            String value = jedis.get(key);

            if(value.isEmpty())
            {
                throw new NullPointerException();
            }

            response.setValue(value);
            response.setStatus(KVStoreStatus.OK);
            return response;
        }
        catch(Exception ex)
        {
            if(ex.getClass()== NullPointerException.class)
            {
                response.setStatus(KVStoreStatus.EITEMNOTFOUND);
                response.setValue(null);
                return response;
            }

            response.setStatus(KVStoreStatus.EPUTFAILED);
            response.setValue(null);
            return  response;
        }
    }

    @Override
    public GetListResponse GetList(String key) throws TException {
        try
        {
            List<String> values = jedis.lrange(key,0,-1);

            if(values.isEmpty())
            {
                throw new NullPointerException();
            }

            listResponse.setValues(values);
            listResponse.setStatus(KVStoreStatus.OK);
            return listResponse;
        }
        catch(Exception ex)
        {

            if(ex.getClass()==NullPointerException.class)
            {
                listResponse.setValues(null);
                listResponse.setStatus(KVStoreStatus.EITEMNOTFOUND);
                return listResponse;
            }

            listResponse.setValues(null);
            listResponse.setStatus(KVStoreStatus.EPUTFAILED);
            return listResponse;
        }
    }

    @Override
    public KVStoreStatus Put(String key, String value) throws TException {

        try
        {
           if(!jedis.set(key,value).equals("OK"))
           {
               throw new Exception();
           }
           return KVStoreStatus.OK;
        }
        catch (Exception ex)
        {
          return KVStoreStatus.EPUTFAILED;
        }
    }

    @Override
    public KVStoreStatus AddToList(String key, String value) throws TException {

        try
        {
           if(!(jedis.lpush(key,value)>0))
           {
              throw new Exception();
           }
           return KVStoreStatus.OK;
        }
        catch (Exception ex)
        {
            return KVStoreStatus.EPUTFAILED;
        }
    }

    @Override
    public KVStoreStatus RemoveFromList(String key, String value) throws TException {

        try
        {
            if(!(jedis.lrem(key,1,value)>0))
            {
                throw new Exception();
            }

            return KVStoreStatus.OK;
        }
        catch(Exception ex)
        {
           return KVStoreStatus.EPUTFAILED;
        }
    }


}
