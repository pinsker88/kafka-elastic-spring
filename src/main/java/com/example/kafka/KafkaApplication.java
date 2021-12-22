package com.example.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class KafkaApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(KafkaApplication.class, args);
	}

	@Autowired
	private KafkaTemplate kafkaTemplate;

	@Override
	public void run(String... args) throws Exception {
		for( int i = 0 ; i < 100 ; ++i) {
			kafkaTemplate.send("topic2", "message" + i);
		}
	}
}

