package com.github.lczupryn.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }
    private ConsumerDemoWithThread(){

    }
    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-6th-app";
        String topic = "first_topic";
        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating the consumer thread");
        Runnable myConsumerThread = new ConsumerThread(
                latch,
                bootstrapServer,
                groupId,
                topic
        );
        Thread myThread = new Thread(myConsumerThread);
        myThread.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);

        }finally {
            logger.info("Application is closing");
        }
        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerThread) myConsumerThread).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");

        }

        ));

    }
    public class ConsumerThread implements Runnable{

        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());

        public ConsumerThread(CountDownLatch latch,
                              String bootstrapServer,
                              String groupId,
                              String topic){
            this.latch = latch;

            //create Consumer config
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

            //create consumer
            consumer = new KafkaConsumer<String, String>(properties);

            //subscribe the consumer to the topic
            consumer.subscribe(Collections.singleton(topic));
        }
        @Override
        public void run() {
            try{
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String , String> record :records){
                    logger.info("Key : "+ record.key() + " Value : " + record.value() + "\n" +
                            "Partition : " + record.partition() + "\n" +
                            "Offset : " + record.offset());

                }
            }}catch (WakeupException e){
                logger.info("Received ");
            }finally {
                consumer.close();
                //tell the main code we are done with consumer
                latch.countDown();
            }

        }
        public void shutdown(){
            //method to interrupt consumer.poll()
            consumer.wakeup();

        }
    }
}

