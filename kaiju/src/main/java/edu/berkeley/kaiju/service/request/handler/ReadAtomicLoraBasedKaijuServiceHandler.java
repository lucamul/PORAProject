package edu.berkeley.kaiju.service.request.handler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import edu.berkeley.kaiju.config.Config;
import edu.berkeley.kaiju.data.DataItem;
import edu.berkeley.kaiju.data.ItemVersion;
import edu.berkeley.kaiju.data.LastValue;
import edu.berkeley.kaiju.exception.HandlerException;
import edu.berkeley.kaiju.net.routing.OutboundRouter;
import edu.berkeley.kaiju.service.request.RequestDispatcher;
import edu.berkeley.kaiju.service.request.message.KaijuMessage;
import edu.berkeley.kaiju.service.request.message.request.*;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;
import edu.berkeley.kaiju.util.Timestamp;
import java.util.concurrent.ConcurrentMap;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;


public class ReadAtomicLoraBasedKaijuServiceHandler extends ReadAtomicKaijuServiceHandler {

    private ConcurrentMap<String,Long> last;
    public ReadAtomicLoraBasedKaijuServiceHandler(RequestDispatcher dispatcher){
        super(dispatcher);
        last = Maps.newConcurrentMap();
    }

    
    public void addLast(final String key, final Long value, final List<String> keyList) throws HandlerException{
        try{
            this.last.entrySet().parallelStream().onClose(()->{keyList.parallelStream().forEach(k -> this.last.putIfAbsent(k, value)); this.last.putIfAbsent(key, value);}).filter((e) -> (e.getKey() == key || keyList.contains(e.getKey()))).forEach(e-> e.setValue((e.getValue() > value) ? e.getValue() : value));        
        }catch(Exception e){
            throw new HandlerException("Error updating Last", e);
        }
    }

    public long getLastTimestamp(final String key){
        return (this.last.containsKey(key)) ? this.last.get(key) : Timestamp.NO_TIMESTAMP;
    }
    
    public void prepare_all(Map<String, byte[]> keyValuePairs, long timestamp) throws HandlerException{
        try {
            // generate a timestamp for this transaction
            // group keys by responsible server.
            Map<Integer, Collection<String>> keysByServerID = OutboundRouter.getRouter().groupKeysByServerID(keyValuePairs.keySet());
            Map<Integer, KaijuMessage> requestsByServerID = Maps.newHashMap();

            for(int serverID : keysByServerID.keySet()) {
                Map<String, DataItem> keyValuePairsForServer = Maps.newHashMap();
                for(String key : keysByServerID.get(serverID)) {
                    keyValuePairsForServer.put(key, instantiateKaijuItem(keyValuePairs.get(key),
                                                                         keyValuePairs.keySet(),
                                                                         timestamp));
                }

                requestsByServerID.put(serverID, new PreparePutAllRequest(keyValuePairsForServer));
            }

            // execute the prepare phase and check for errors
            Collection<KaijuResponse> responses = dispatcher.multiRequest(requestsByServerID);
            KaijuResponse.coalesceErrorsIntoException(responses);
        } catch (Exception e) {
            throw new HandlerException("Error processing request", e);
        }
    }

    public void commit_all(Map<String, byte[]> keyValuePairs,long timestamp) throws HandlerException{
        try{
            Map<Integer, Collection<String>> keysByServerID = OutboundRouter.getRouter().groupKeysByServerID(keyValuePairs.keySet());
            Map<Integer, KaijuMessage> requestsByServerID = Maps.newHashMap();
            for(int serverID : keysByServerID.keySet()) {
                requestsByServerID.put(serverID,  new CommitPutAllRequest(timestamp));
            }

            // this is only for the experiment in Section 5.3 and will trigger CTP
            if(dropCommitPercentage != 0 && random.nextFloat() < dropCommitPercentage) {
                int size = keysByServerID.size();
                int item = random.nextInt(size);
                int i = 0;
                for(int serverID : keysByServerID.keySet())
                {
                    if (i == item) {
                        requestsByServerID.remove(serverID);
                        break;
                    }

                    i++;
                }
            }
            if(requestsByServerID.isEmpty()) {
                return;
            }
            Collection<KaijuResponse> responses1 = dispatcher.multiRequest(requestsByServerID);
            KaijuResponse.coalesceErrorsIntoException(responses1);
        }catch(Exception e){
            throw new HandlerException("Error processing request",e);
        }
    }

    public Map<String,byte[]> get_all(List<String> keys) throws HandlerException{
        try{
            Map<String,Long> get_set = Maps.newHashMap();
            List<String> get_set_no_ts = Lists.newArrayList();
            for(String k : keys){
                long timestamp = getLastTimestamp(k);
                if(timestamp == Timestamp.NO_TIMESTAMP || timestamp < 0){
                    get_set_no_ts.add(k);
                    continue;
                }
                get_set.put(k, timestamp);
            }
            Collection<KaijuResponse> responses = fetch_by_version_from_server(get_set);
            Collection<KaijuResponse> responses2 = fetch_from_server(get_set_no_ts);
            responses.addAll(responses2);
            Map<String,byte[]> ret = Maps.newHashMap();


            for(KaijuResponse response : responses){
                for(Map.Entry<String,DataItem> keyValuePair : response.keyValuePairs.entrySet()){
                    if(keyValuePair == null || keyValuePair.getValue() == null || keyValuePair.getKey() == null || keyValuePair.getValue().getValue() == null) continue;
                    ret.put(keyValuePair.getKey(), keyValuePair.getValue().getValue());
                    if(keyValuePair.getValue().getTimestamp() < Timestamp.NO_TIMESTAMP) continue;
                    if(keyValuePair.getValue().getTransactionKeys() == null || keyValuePair.getValue().getTransactionKeys().isEmpty()){
                        addLast(keyValuePair.getKey(), keyValuePair.getValue().getTimestamp(), Lists.newArrayList());
                        continue;
                    }
                    List<String> ks = Lists.newArrayList();
                    ks.addAll(keyValuePair.getValue().getTransactionKeys());
                    addLast(keyValuePair.getKey(),keyValuePair.getValue().getTimestamp(), ks);
                }
            }
            return ret;
        }catch(Exception e){
            throw new HandlerException("Error processing request",e);
        }
    }
    
    public DataItem instantiateKaijuItem(byte[] value,
                                        Collection<String> allKeys,
                                        long timestamp) {
        return new DataItem(timestamp, value, Lists.newArrayList(allKeys));
    }
}
