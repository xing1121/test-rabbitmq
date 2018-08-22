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
 * 		其中的公平模式（能者多劳模式Fair dispatch）：每次队列只发送一条消息给消费者（设置队列的basicQos(1)），消费者设置手动ack，当一条消息消费完并成功反馈给服务端，才会接收下一条消息。
 * @author 80002888
 * @date   2018年8月22日
 */
public class WorkFair {
	
	private static final String TASK_QUEUE_NAME = "task_queue_fair";
	
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
		channel.queueDeclare(TASK_QUEUE_NAME, false, false, false, null);
		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
		// 4.设置为work模式（参数设为1：每次只接受一条消息，这时要关闭自动反馈，要在任务结束后手动反馈）
		channel.basicQos(1);
		// 5.定义消费者（启动多个消费者，其中有sleep(0)的和sleep(1000)的，能明显看出能者多劳的模式）
		int r = (new Random().nextInt(1000)) % 2;
		System.out.println(r == 0 ? "work slow..." : "work fast...");
		Consumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {
				try {
					String message = new String(body, "UTF-8");
					try {
						Thread.sleep(r == 0 ? 1000 : 0);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					System.out.println(" [x] Received '" + message + "'");
				} finally {
					System.out.println(" [x] Done");
					// 手动在消费完毕时进行反馈
					channel.basicAck(envelope.getDeliveryTag(), false);
				}
			}
		};
		// 6.消费消息（第二个参数为自动给服务器端的反馈：为false时，服务器无法确定消息被消费不从queue中删除，需要手动应答）
		channel.basicConsume(TASK_QUEUE_NAME, false, consumer);
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
