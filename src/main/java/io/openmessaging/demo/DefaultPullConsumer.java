package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

import java.util.*;

public class DefaultPullConsumer implements PullConsumer {
    private MessageStore messageStore = MessageStore.getInstance();
    private KeyValue properties;
    private String queue;
    private Set<String> buckets = new HashSet<>();
    private List<String> bucketList = new LinkedList<>();

    private int lastIndex = 0;

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        messageStore.setProperties(properties);
    }


    @Override public KeyValue properties() {
        return properties;
    }


    @Override public Message poll() {
        //Todo:
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

    @Override public synchronized void attachQueue(String queueName, Collection<String> topics) {
        //Todo:

        if (queue != null && !queue.equals(queueName)) {
            throw new ClientOMSException("You have alreadly attached to a queue " + queue);
        }
        queue = queueName;
        buckets.add(queueName);
        buckets.addAll(topics);
        bucketList.clear();
        bucketList.addAll(buckets);
    }


}
