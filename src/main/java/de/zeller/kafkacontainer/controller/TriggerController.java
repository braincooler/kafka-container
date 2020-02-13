package de.zeller.kafkacontainer.controller;


import de.zeller.kafkacontainer.kafka.MyKafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TriggerController {

    @Autowired
    private MyKafkaConsumer myKafkaConsumer;

    @GetMapping(path = "start")
    private void execute(){
        System.out.println("start...");
        myKafkaConsumer.start("key");
    }
}
