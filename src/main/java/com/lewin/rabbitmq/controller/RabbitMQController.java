package com.lewin.rabbitmq.controller;

import com.rabbitmq.client.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

@RestController
@RequestMapping("/rabbitmq")
public class RabbitMQController {

    //队列名称
    private final static String QUEUE_NAME = "hello";

    @Value("${server.port}")
    String port;
    @RequestMapping("/hello")
    public String home(@RequestParam String name) {
        return "hi "+name+",i am from port:" +port;
    }

    /**
     * 消息生产者
     * @param name
     */
    @RequestMapping("/put")
    public void put(@RequestParam(value="name", defaultValue=QUEUE_NAME) String name){
        /**
         * 创建连接连接到MabbitMQ
         */
        ConnectionFactory factory = new ConnectionFactory();
        //设置MabbitMQ所在主机ip或者主机名
        factory.setHost("192.168.3.144");
        factory.setUsername("root");
        factory.setPassword("123456");
        factory.setVirtualHost("/");
        factory.setPort(5672);//默认监听端口是5672，15672是管理界面端口

        Connection connection = null;

        Channel channel = null;
        try {
            //创建一个连接
            connection = factory.newConnection();
            //创建一个频道
            channel = connection.createChannel();
            //指定一个队列
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            //发送的消息
            String message = "hello world!";
            //往队列中发出一条消息
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
            System.out.println("Producter Sent '" + message + "'");
            //关闭频道和连接
            channel.close();
            connection.close();
        }catch (Exception e){
            e.printStackTrace();
            System.out.println(e.toString());
        }
    }

    /**
     * 消息消费者
     * @param name
     */
    @RequestMapping("/get")
    public void get(@RequestParam(value="name", defaultValue=QUEUE_NAME) String name){
        //打开连接和创建频道，与发送端一样
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.3.144");
        factory.setUsername("root");
        factory.setPassword("123456");
        factory.setVirtualHost("/");
        factory.setPort(5672);
        Connection connection = null;
        Channel channel = null;
        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
            //声明队列，主要为了防止消息接收者先运行此程序，队列还不存在时创建队列。
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            System.out.println("Customer Waiting for messages. To exit press CTRL+C");

            //DefaultConsumer类实现了Consumer接口，通过传入一个频道，
            // 告诉服务器我们需要那个频道的消息，如果频道中有消息，就会执行回调函数handleDelivery
            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body)
                        throws IOException {
                    String message = new String(body, "UTF-8");
                    System.out.println("Customer Received '" + message + "'");
                }
            };
            //自动回复队列应答 -- RabbitMQ中的消息确认机制
            channel.basicConsume(QUEUE_NAME, true, consumer);

//            //创建队列消费者
//            QueueingConsumer consumer = new QueueingConsumer(channel);
//            //指定消费队列
//            channel.basicConsume(QUEUE_NAME, true, consumer);
//            while (true) {
//                //nextDelivery是一个阻塞方法（内部实现其实是阻塞队列的take方法）
//                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
//                String message = new String(delivery.getBody());
//                System.out.println(" [x] Received '" + message + "'");
//            }
        }catch (Exception e){
            e.printStackTrace();
            System.out.println(e.toString());
        }

    }
}
