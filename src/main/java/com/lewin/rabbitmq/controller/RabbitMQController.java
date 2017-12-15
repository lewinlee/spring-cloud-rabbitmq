package com.lewin.rabbitmq.controller;

import com.rabbitmq.client.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

/**
 * @author lewin
 * @from https://www.cnblogs.com/stormli/p/rabbitmq.html
 */
@RestController
@RequestMapping("/rabbitmq")
public class RabbitMQController {

    //单个消息队列名称
    private final static String QUEUE_NAME_SINGLE = "SINGLE";
    //广播消息队列
    private final static String QUEUE_NAME_BROADCAST = "BROADCAST";

    private final static String MESSAGE = "hello world";


    @Value("${server.port}")
    String port;
    @RequestMapping("/hello")
    public String home(@RequestParam String name) {
        return "hi "+name+",i am from port:" +port;
    }



    /**
     * 例子1
     * 消息生产者
     * @param message
     */
    @RequestMapping("/put")
    public void put(@RequestParam(value="message", defaultValue=MESSAGE) String message){
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
            //指定一个队列，默认，向指定的队列发送消息，消息只会被一个consumer处理,多个消费者消息会轮训处理,消息发送时如果没有consumer，消息不会丢失
            //参数1：队列名称
            //参数2：为true时server重启队列不会消失
            //参数3：队列是否是独占的，如果为true只能被一个connection使用，其他连接建立时会抛出异常
            //参数4：队列不再使用时是否自动删除（没有连接，并且没有未处理的消息)
            //参数5：建立队列时的其他参数
            channel.queueDeclare(QUEUE_NAME_SINGLE, false, false, false, null);
            //发送的消息

            //往队列中发出一条消息
            channel.basicPublish("", QUEUE_NAME_SINGLE, null, message.getBytes());
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
     * 例子1
     * 消息消费者
     * http://localhost:8081/rabbitmq/get
     * http://localhost:8081/rabbitmq/get?name=BROADCAST
     */
    @RequestMapping("/get")
    //如果无输入，默认取QUEUE_NAME_SINGLE消息队列名
    public void get(@RequestParam(value="name", defaultValue=QUEUE_NAME_SINGLE) String name){
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
            channel.queueDeclare(name, false, false, false, null);
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
            channel.basicConsume(QUEUE_NAME_SINGLE, true, consumer);

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

    /**
     * 例子2
     * 消息生产者
     * http://localhost:8081/rabbitmq/putBroadcast
     * @param name
     */
    @RequestMapping("/putBroadcast")
    public void putBroadcast(@RequestParam(value="name", defaultValue=QUEUE_NAME_BROADCAST) String name,
                             @RequestParam(value="message", defaultValue=MESSAGE) String message){
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
            //广播模式，广播给所有队列  接收方也必须通过fanout交换机获取消息,所有连接到该交换机的consumer均可获取消息
            //如果producer在发布消息时没有consumer在监听，消息将被丢弃

            //定义一个交换机
            //参数1：交换机名称
            //参数2：交换机类型
            //参数3：交换机持久性，如果为true则服务器重启时不会丢失
            //参数4：交换机在不被使用时是否删除
            //参数5：交换机的其他属性
            channel.exchangeDeclare(QUEUE_NAME_BROADCAST, "fanout", true, true, null);

            //发送一条广播消息,参数2此时无意义
            channel.basicPublish(QUEUE_NAME_BROADCAST, "", null, message.getBytes());

            System.out.println("Producter Broadcast Send " + message);
            //关闭频道和连接
            channel.close();
            connection.close();

        }catch (Exception e){
            e.printStackTrace();
            System.out.println(e.toString());
        }
    }


}
