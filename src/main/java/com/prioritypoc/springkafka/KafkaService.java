package com.prioritypoc.springkafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaService {

    @Autowired
    private MessageProducer producer;

    @GetMapping("/sendkafkamessage")
    String all(@Nullable @RequestParam String onlineMessage,
               @Nullable @RequestParam String batchMessage
    ) throws InterruptedException {

        //this is just an additional loop to send more records into online topic
       if(onlineMessage != null){
           for(int i=20; i<31; i++)
          producer.sendToOnlineTopic(onlineMessage+">> "+i+" <<");
       }

       if(batchMessage != null)
           for(int k=20; k<31; k++)
            producer.sendToBatchTopic(batchMessage+">> "+k+" <<");

        return "Successfully sent online message >>"+onlineMessage+" and batch message >>"+batchMessage;
    }

}
