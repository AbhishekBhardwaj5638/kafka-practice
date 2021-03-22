package org.example.enrichment;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.ibm.gbs.schema.Customer;
import com.ibm.gbs.schema.CustomerBalance;
import com.ibm.gbs.schema.Transaction;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class CustomerBalanceEnrichment {


    private static Gson gson = new GsonBuilder().create();

    public static void main(String[] args) {/**/

        // stream of data for customers
        // initializing the default stream processing property
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "customer-account-enrichment1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        props.put(
//                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        // When configuring the default serdes of StreamConfig

       props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        props.put("schema.registry.url", "http://my-schema-registry:8081");


//// When you want to override serdes explicitly/selectively
//        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
//                "http://my-schema-registry:8081");
//// `Foo` and `Bar` are Java classes generated from Avro schemas
//        final Serde<Customer> keySpecificAvroSerde = new SpecificAvroSerde<>();
//        keySpecificAvroSerde.configure(serdeConfig, true); // `true` for record keys
//
//        final Serde<Transaction> valueSpecificAvroSerde = new SpecificAvroSerde<>();
//        valueSpecificAvroSerde.configure(serdeConfig, false); // `false` for record values
//
//        StreamsBuilder builder = new StreamsBuilder();
//        KStream<Customer, Transaction> textLines = builder.stream("my-avro-topic", Consumed.with(keySpecificAvroSerde, valueSpecificAvroSerde));




//
////
////        final Serde<String> stringSerde = Serdes.String();
////        final Serde<Long> longSerde = Serdes.Long();
//
          final StreamsBuilder builder = new StreamsBuilder();
//
//        KStream<String,Customer> customerKStream = builder.stream("customer1",Consumed.with(Serdes.String(),getSpecificAvroSerde(props)));
//
//        KStream<String,Transaction> TransactionKStream = builder.stream("transaction",Consumed.with(Serdes.String(),getSpecificAvroSerde(props)));
//

        KStream<String, String> customerInfo =
                builder.stream("customer1"); //  custom info data but it doesn't have any key defined

        // generating another stream with account id as key which is derived from customer info
        KStream<CharSequence, String> accountCustomerKeyStream = customerInfo.selectKey((key, value) -> {
            Customer customer = gson.fromJson(value, Customer.class);
            return customer.getAccountId();
        });

        KStream<String, String> accountBalInfo = builder.stream("transaction");

        KStream<CharSequence, String> accountBalInfoKeyStream = accountBalInfo.selectKey((key, value) -> {
            Transaction accountBalance = gson.fromJson(value, Transaction.class);
            return accountBalance.getAccountId();
        });


        KStream<CharSequence, String> accountCustomerInfoStream = accountBalInfoKeyStream.join(
                accountCustomerKeyStream,
                (value1, value2) -> {
                    Transaction accountBalance = gson.fromJson(value1, Transaction.class);
                    Customer customer = gson.fromJson(value2, Customer.class);
                    CustomerBalance balance = new CustomerBalance(accountBalance.getAccountId(), customer.getCustomerId(),customer.getPhoneNumber(),accountBalance.getBalance());
                    return gson.toJson(balance);
                },
                JoinWindows.of(Duration.ofSeconds(3600 * 24)));

        accountCustomerInfoStream.print(Printed.toSysOut());
        // stream of account balances

        accountCustomerInfoStream.to("account-bal-customer-info");
        final Topology topology = builder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        // output stream where account is enriched with customer info


    }

    static  <T extends SpecificRecord> SpecificAvroSerde<T> getSpecificAvroSerde(final Properties envProps){
        final SpecificAvroSerde<T> specificAvroSerde = new SpecificAvroSerde<>();
        final HashMap<String,String> serdeConfig= new HashMap<>();
        serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG,envProps.getProperty("schema.registry.url"));
        specificAvroSerde.configure(serdeConfig,false);
        return specificAvroSerde;
    }
}