package com.sf.wdx.work;

import java.io.IOException;
import java.util.Date;
import java.util.Random;

import org.junit.Test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.sf.wdx.util.ConnectionUtil;
import com.sf.wdx.util.DateUtils;

/**
 * 描述：RabbitMQ的Work模式（又叫task-worker模式）
 * 		其中的轮询模式（Round-robin dispatching）：消息按顺序平分给所有消费者（默认basicQos(0)：轮询分配消息给消费者），可能出现消息在消费者堆积。
 * 			即使消费者中有的消费慢，有的消费快，消息依然是平分
 * @author 80002888
 * @date   2018年8月20日
 */
public class WorkRoundRobin {
	
	private static final String TASK_QUEUE_NAME = "task_queue_roundrobin";
	
	private final static String PATTERN = "yyyy-MM-dd HH:mm:ss";
	
	/**
	 * 消费者
	 *	@ReturnType	void 
	 *	@Date	2018年8月20日	下午3:14:49
	 *  @Param  @throws Exception
	 */
	@Test
	public void consumer() throws Exception{
		// 1.获取连接
		Connection connection = ConnectionUtil.getConnection();
		// 2.获取通道
		Channel channel = connection.createChannel();
		// 3.绑定队列
		channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
		// 4.设置为work模式中的轮询模式（不设置的话，默认也是0）
		channel.basicQos(0);
		// 5.定义消费者（启动多个消费者）
		int r = (new Random().nextInt(1000)) % 2;
		System.out.println(r == 0 ? "work slow..." : "work fast...");
		Consumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
				String message = new String(body, "UTF-8");
				try {
					Thread.sleep(r == 0 ? 1000 : 0);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.out.println(" [x] Received '" + message + "'");
			}
		};
		// 6.消费消息（自动反馈）
		channel.basicConsume(TASK_QUEUE_NAME, true, consumer);
		System.in.read();
		// 7.关闭
		channel.close();
		connection.close();
	}
	
	/**
	 * 生产者
	 *	@ReturnType	void 
	 *	@Date	2018年8月20日	下午3:14:38
	 *  @Param  @throws Exception
	 */
	@Test
	public void producer() throws Exception{
		// 1.获取连接
		Connection connection = ConnectionUtil.getConnection();
		// 2.获取通道
		Channel channel = connection.createChannel();
		// 3.绑定队列
		channel.queueDeclare(TASK_QUEUE_NAME, false, false, false, null);
		// 4.发送消息
		for (int i = 0; i < 100; i++) {
			Thread.sleep(200);
			String message = "Hello World!" + DateUtils.date2Str(new Date(), PATTERN) + "----" + i;
			channel.basicPublish("", TASK_QUEUE_NAME, null, message.getBytes("UTF-8"));
			System.out.println(" [x] Sent '" + message + "'");
		}
		// 5.关闭
		channel.close();
		connection.close();
	}
	
}

