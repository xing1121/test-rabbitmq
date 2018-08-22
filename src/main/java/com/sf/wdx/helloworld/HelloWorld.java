package com.sf.wdx.helloworld;

import java.io.IOException;
import java.util.Date;

import org.junit.Test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.sf.wdx.util.DateUtils;

/**
 * 描述：RabbitMQ的HelloWorld，一个生产者、一个消费者、一个队列
 *			生产者    --message-->    队列     --message-->    消费者 
 * @author 80002888
 * @date   2018年8月20日
 */
public class HelloWorld {

	private final static String QUEUE_NAME = "hello";
	
	private final static String PATTERN = "yyyy-MM-dd HH:mm:ss";
	
	/**
	 * 消费者从RabbitMQ接收数据，如果只有一条消息，启动多个消费者只会有一个收到消息
	 *	@ReturnType	void 
	 *	@Date	2018年8月20日	上午10:51:58
	 *  @Param  @throws Exception
	 */
	@Test
	public void consumer() throws Exception{
		// 1.获取连接工厂
	    ConnectionFactory factory = new ConnectionFactory();
	    // 2.设置服务器IP地址
	    factory.setHost("localhost");
	    // 3.获取连接
	    Connection connection = factory.newConnection();
	    // 4.创建一个通道
	    Channel channel = connection.createChannel();
	    // 5.绑定队列
	    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
	    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
	    // 6.创建消费者
	    Consumer consumer = new DefaultConsumer(channel) {
	      @Override
	      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
	          throws IOException {
	        String message = new String(body, "UTF-8");
	        System.out.println(" [x] Received '" + message + "'");
	      }
	    };
	    // 7.把消费者绑定到指定通道的指定队列中，监听消息
    	channel.basicConsume(QUEUE_NAME, true, consumer);
    	System.in.read();
    	// 8.关闭
    	channel.close();
    	connection.close();
	}
	
	/**
	 * 生产者向RabbitMQ发送数据
	 *	@ReturnType	void 
	 *	@Date	2018年8月20日	上午10:51:41
	 *  @Param  @throws Exception
	 */
	@Test
	public void producer() throws Exception{
		// 1.获取连接工厂
		ConnectionFactory factory = new ConnectionFactory();
		// 2.设置服务器IP地址
		factory.setHost("localhost");
		// 3.获取连接
		Connection connection = factory.newConnection();
		// 4.创建一个通道
		Channel channel = connection.createChannel();
		// 5.绑定队列
		channel.queueDeclare(QUEUE_NAME, false, false, false, null);
		// 6.发送消息
		for (int i = 0; i < 100; i++) {
			Thread.sleep(1000);
			String message = "Hello World!" + DateUtils.date2Str(new Date(), PATTERN);
			channel.basicPublish("", QUEUE_NAME, null, message.getBytes("UTF-8"));
			System.out.println(" [x] Sent '" + message + "'");
		}
		// 7.关闭
		channel.close();
		connection.close();
	}
	
}
