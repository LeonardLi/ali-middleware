package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

import java.lang.reflect.Array;
import java.util.*;

public class DefaultPullConsumer implements PullConsumer {
    private MessageStore messageStore = MessageStore.getInstance();
    static KeyValue properties;
    private String queue;
    //private Set<String> buckets = new HashSet<>();
    //private List<String> bucketList = new LinkedList<>();

    //本线程的所对应的Queue和Topics
    private ThreadLocal<ArrayList<String>> threadBucketList = new ThreadLocal<>();
    private ArrayList<String> bucketList = new ArrayList<>();
    private int lastIndex = 0;

    public KeyValue getProperties() {
        return properties;
    }

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        MessageStore.getInstance().setProperties(properties);
        MessageStore.getInstance().initFileArray();
        //threadBucketList.set(new ArrayList<>());
    }

    @Override public KeyValue properties() {
        return properties;
    }

    @Override public Message poll() {
        //Todo:
        //ArrayList<String> bucketList = threadBucketList.get();
//        if (buckets.size() == 0 || queue == null) {
//            return null;
//        }
//
//        for (int i = 0; i < bucketList.size(); i++) {
//            Message message = messageStore.pullMessage(queue, bucketList.get(i));
//            if (message != null) {
//                return message;
//            }
//        }
//        return null;
        if (bucketList.size() == 0 || queue == null){
            return null;
        }
        for (int i = 0; i < bucketList.size(); i++){
            Message message = messageStore.pullMessage(queue, bucketList.get(i));
            if(message == null) bucketList.remove(i);
            if(message != null) {
                return message;
            }
        }
        return null;

    }

    @Override public Message poll(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void attachQueue(String queueName, Collection<String> topics) {
        //Todo:
        //ArrayList<String> bucketList = threadBucketList.get();
        //threadBucketList.set(bucketList);

        if (queue != null && !queue.equals(queueName)) {
            throw new ClientOMSException("You have alreadly attached to a queue " + queue);
        }
        queue = queueName;
        HashSet buckets = new HashSet();
        buckets.add(queueName);
        buckets.addAll(topics);
        bucketList.clear();
        bucketList.addAll(buckets);
    }


}
