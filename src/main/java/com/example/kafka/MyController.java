package com.example.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;

@Controller
public class MyController {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public MyController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping(path = "/lala")
    public @ResponseBody ResponseEntity<String> getResponse() {
        kafkaTemplate.send("topic1","Natan");


        return new ResponseEntity<String>("sended", HttpStatus.OK);
    }

    @KafkaListener(topics = "topic1", groupId = "uncommitted", containerFactory = "manualCommitKafkaListenerContainerFactory")
    public void receiveMessageAndDontCommit(ConsumerRecord<String, String> message) {
        System.out.println("ReceivedMessage manual commit uncommitted= " + message.value());
    }

    @KafkaListener(topics = "topic1", groupId = "committed", containerFactory = "manualCommitKafkaListenerContainerFactory")
    public void receiveMessageAndCommit(ConsumerRecord<String, String> message, Acknowledgment acknowledgment) {
        System.out.println("ReceivedMessage manual commit committed = " + message.value());
        acknowledgment.acknowledge();
    }

    @KafkaListener(topics = "topic1", groupId = "autocommit", containerFactory = "autoCommitKafkaListenerContainerFactory")
    public void receiveMessageAutoCommit(ConsumerRecord<String, String> message) {
        System.out.println("ReceivedMessage auto commit = " + message.value());
    }

    @KafkaListener(id = "batch-listener", topics = "topic2", containerFactory = "batchKafkaListenerContainerFactory")
    public void receive(@Payload List<String> messages,
                        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) List<Integer> partitions,
                        @Header(KafkaHeaders.OFFSET) List<Long> offsets) {

        System.out.println("beginning to consume batch messages =>>>> " + messages.size());
    }
}
