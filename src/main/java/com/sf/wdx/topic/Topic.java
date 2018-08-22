package com.sf.wdx.topic;

import java.io.IOException;
import java.util.Date;
import java.util.Random;

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
 * 描述：通配符模式（topic交换机）
 * 			routingKey类似：quick.orange.rabbit
 * 						   quick.orange.*
 * 						   #.rabbit
 * 						   *匹配一个单词
 * 						   #匹配多个单词
 * @author 80002888
 * @date   2018年8月21日
 */
public class Topic {

	public static final String EXCHANGE_NAME = "logs_topic";
	
	private static final String INFO = "info";
	private static final String WARN = "warn";
	private static final String ERROR = "error";
	
	private static final String CN = "cn";
	private static final String US = "us";
	private static final String KR = "kr";
	
	private static final String MING = "ming";
	private static final String HONG = "hong";
	private static final String LI = "li";
	
	private static final String[] LEVELS = new String[]{INFO, WARN, ERROR}; 
	private static final String[] COUNTRYS = new String[]{CN, US, KR};
	private static final String[] PERSONS = new String[]{MING, HONG, LI};
	
	private final static String PATTERN = "yyyy-MM-dd HH:mm:ss";
	
	/**
	 * 消费者二号，接收routingKey为*.ming.*的数据
	 *	@ReturnType	void 
	 *	@Date	2018年8月21日	下午4:56:59
	 *  @Param  @throws Exception
	 */
	@Test
	public void consumerTwo() throws Exception{
		// 1.获取连接
		Connection connection = ConnectionUtil.getConnection();
		// 2.创建一个通道
		Channel channel = connection.createChannel();
	    // 3.声明随机队列（服务器自动生成，消费者停止时自动删除，非持久化）
		String queueName = channel.queueDeclare().getQueue();
		// 4.声明交换机（topic：通配符模式）
		channel.exchangeDeclare(EXCHANGE_NAME, ExchangeTypes.TOPIC);
		// 5.绑定队列到交换机
		String routingKey = "*." + MING + ".*";
		channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
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
	 * 消费者一号，接收routingKey为cn.#的数据和*.*.error的数据
	 *	@ReturnType	void 
	 *	@Date	2018年8月21日	下午4:56:28
	 *  @Param  @throws Exception
	 */
	@Test
	public void consumerOne() throws Exception{
		// 1.获取连接
		Connection connection = ConnectionUtil.getConnection();
		// 2.创建一个通道
		Channel channel = connection.createChannel();
	    // 3.声明随机队列（服务器自动生成，消费者停止时自动删除，非持久化）
		String queueName = channel.queueDeclare().getQueue();
		// 4.声明交换机（topic：通配符模式）
		channel.exchangeDeclare(EXCHANGE_NAME, ExchangeTypes.TOPIC);
		// 5.绑定队列到交换机
		String routingKey1 = CN + ".#";
		String routingKey2 = "*.*." + ERROR;
		channel.queueBind(queueName, EXCHANGE_NAME, routingKey1);
		channel.queueBind(queueName, EXCHANGE_NAME, routingKey2);
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
	 * 生产者，向topic交换机发送消息，routingKey:<country>.<person>.<level>		例：cn.li.warn
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
		channel.exchangeDeclare(EXCHANGE_NAME, ExchangeTypes.TOPIC);
		// 4.发送消息到交换机（随机发送3*3*3种routingKey的消息）
		for (int i = 0; i < 100; i++) {
			Thread.sleep(200);
			String country = COUNTRYS[new Random().nextInt(1000) % 3];
			String person = PERSONS[new Random().nextInt(1000) % 3];
			String level = LEVELS[new Random().nextInt(1000) % 3];
			String routingKey = country + "." + person + "." + level;
			String message = routingKey + "---" + DateUtils.date2Str(new Date(), PATTERN) + "---" + i;
			channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes());
			System.out.println(" [x] Sent '" + message + "'");
		}
		// 5.关闭
		channel.close();
		connection.close();
	}
}
