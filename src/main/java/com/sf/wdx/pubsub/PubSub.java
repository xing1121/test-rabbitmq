package com.sf.wdx.pubsub;

import java.io.IOException;
import java.util.Date;

import org.junit.Test;
import org.springframework.amqp.core.ExchangeTypes;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.sf.wdx.util.ConnectionUtil;
import com.sf.wdx.util.DateUtils;

/**
 * 描述：订阅发布模式（广播模式broadcast）（fanout交换机）
 * @author 80002888
 * @date   2018年8月21日
 */
public class PubSub {

	public static final String EXCHANGE_NAME = "logs";
	
	private final static String PATTERN = "yyyy-MM-dd HH:mm:ss";
	
	/**
	 * 消费者，生成服务器随机命名的队列从交换机接收消息
	 *	@ReturnType	void 
	 *	@Date	2018年8月21日	下午2:34:57
	 *  @Param  @throws Exception
	 */
	@Test
	public void consumer() throws Exception{
		// 1.获取连接
		Connection connection = ConnectionUtil.getConnection();
		// 2.创建一个通道
		Channel channel = connection.createChannel();
	    // 3.声明随机队列（服务器自动生成，消费者停止时自动删除，非持久化）
		String queueName = channel.queueDeclare().getQueue();
		// 4.声明交换机（fanout：广播模式。若不存在则创建，存在则使用）
		channel.exchangeDeclare(EXCHANGE_NAME, ExchangeTypes.FANOUT);
		// 5.绑定队列到交换机
		channel.queueBind(queueName, EXCHANGE_NAME, "");
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
	    // 7.把消费者绑定到指定频道的指定队列中，监听消息
    	channel.basicConsume(queueName, true, consumer);
    	System.in.read();
    	// 8.关闭
    	channel.close();
    	connection.close();
	}
	
	/**
	 * 生产者，向交换机发送消息
	 *	@ReturnType	void 
	 *	@Date	2018年8月21日	下午2:34:37
	 *  @Param  @throws Exception
	 */
	@Test
	public void producer() throws Exception{
		// 1.获取连接
		Connection connection = ConnectionUtil.getConnection();
		// 2.创建一个通道
		Channel channel = connection.createChannel();
		// 3.声明交换机（fanout：广播分发模式，若不存在则创建，存在则使用）
		channel.exchangeDeclare(EXCHANGE_NAME, ExchangeTypes.FANOUT);
		// 4.发送消息到交换机
		for (int i = 0; i < 100; i++) {
			Thread.sleep(200);
			String message = DateUtils.date2Str(new Date(), PATTERN) + "---" + i;
			channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
			System.out.println(" [x] Sent '" + message + "'");
		}
		// 5.关闭
		channel.close();
		connection.close();
	}
}
