package com.nicoloboschi.pulsar.transactions;

import lombok.Cleanup;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.transaction.Transaction;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ProduceConsumeTransaction {

    public static void main(String[] args) throws Exception {

        String serviceUrl = "pulsar://localhost:6650";
        if (args.length > 0) {
            serviceUrl = args[0];
        }
        final String topicName = "my-topic-" + UUID.randomUUID();

        @Cleanup final PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .enableTransaction(true)
                .build();
        System.out.println("Connected to " + serviceUrl);

        final Transaction transaction = pulsarClient.newTransaction().build().get();


        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();


        producer.newMessage()
                .value("hellonormal1")
                .send();

        producer.newMessage(transaction)
                .value("hellotrans")
                .send();



        producer.newMessage()
                .value("hellonormal")
                .send();

        transaction.commit().get();
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("sub1")
                .subscribe();
        Message<String> msg;
        while ((msg = consumer.receive(5, TimeUnit.SECONDS)) != null) {
            System.out.println("got: " + msg.getValue() + " seq: " + msg.getSequenceId());
        }
    }
}
