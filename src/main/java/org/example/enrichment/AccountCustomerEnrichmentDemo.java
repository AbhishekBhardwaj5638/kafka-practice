package org.example.enrichment;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.ibm.gbs.schema.Customer;
import com.ibm.gbs.schema.CustomerBalance;
import com.ibm.gbs.schema.Transaction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

import java.time.Duration;
import java.util.Properties;

public class AccountCustomerEnrichmentDemo {

    private static Gson gson = new GsonBuilder().create();

    public static void main(String[] args) {

        // stream of data for customers

        // initializing the default stream processing property
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "customer-account-enrichment1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        props.put(
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<CharSequence, Customer> customerInfo =
                builder.stream("customer1"); //  custom info data but it doesn't have any key defined

        // generating another stream with account id as key which is derived from customer info
        KStream<CharSequence, Customer> accountCustomerKeyStream = customerInfo.selectKey((key, value) -> {
            Customer customer = gson.fromJson(String.valueOf(value), Customer.class);
            return customer.getAccountId();
        });

        KStream<CharSequence, Transaction> accountBalInfo = builder.stream("transaction");

        KStream<CharSequence, Transaction> accountBalInfoKeyStream = accountBalInfo.selectKey((key, value) -> {
            Transaction transaction = gson.fromJson(String.valueOf(value), Transaction.class);
            return transaction.getAccountId();

        });

        KStream<CharSequence, Object> accountCustomerInfoStream = accountBalInfoKeyStream.join(
                accountCustomerKeyStream,
                (value1, value2) -> {
                    Transaction transaction = gson.fromJson(String.valueOf(value1), Transaction.class);
                    Customer customer = gson.fromJson(String.valueOf(value2), Customer.class);
                    CustomerBalance balance = new CustomerBalance(transaction.getAccountId(),customer.getCustomerId(),customer.getPhoneNumber(),transaction.getBalance());
                    return gson.toJson(balance);
                },
                JoinWindows.of(Duration.ofSeconds(3600 * 24)));

        accountCustomerInfoStream.print(Printed.toSysOut());
        // stream of account balances

        accountCustomerInfoStream.to("customer-balance");
        final Topology topology = builder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        // output stream where account is enriched with customer info
    }
}
