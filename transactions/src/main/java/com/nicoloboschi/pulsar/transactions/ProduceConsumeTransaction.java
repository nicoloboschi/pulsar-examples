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
import java.util.concurrent.TimeUnit;

public class ProduceConsumeTransaction {

    public static void main(String[] args) throws Exception {

        String serviceUrl = "pulsar://localhost:6650";
        if (args.length > 0) {
            serviceUrl = args[0];
        }

        @Cleanup final PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .enableTransaction(true)
                .build();
        System.out.println("Connected to " + serviceUrl);

        final Transaction transaction = pulsarClient.newTransaction().build().get();


        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .topic("mytopic")
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();


        producer.newMessage(transaction)
                .value("hello".getBytes(StandardCharsets.UTF_8))
                .send();
        transaction.abort().get();


        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer(Schema.BYTES)
                .topic("mytopic")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("sub1")
                .subscribe();
        final Message<byte[]> receive = consumer.receive(5, TimeUnit.SECONDS);
        System.out.println("got: " + receive);
    }
}
