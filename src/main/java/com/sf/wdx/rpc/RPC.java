package com.sf.wdx.rpc;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.junit.Test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.sf.wdx.util.ConnectionUtil;

/**
 * 描述：测试RabbitMQ的RPC（远程过程调用）---先启动服务端，再启动客户端。客户端发送消息给服务端，服务端接收到消息，并响应消息给客户端，客户端接收到响应消息。
 * @author 80002888
 * @date   2018年8月22日
 */
public class RPC {

	/**
	 * 客户端发送消息的队列名称（也是服务端收到消息的队列）
	 */
	private static final String RPC_QUEUE_NAME = "rpc_queue";

	/**
	 * 客户端，发送请求时携带correlationId和replyQueueName，服务端返回的响应同样要匹配这两个参数。
	 *	@ReturnType	void 
	 *	@Date	2018年8月22日	上午10:14:54
	 *  @Param  @throws Exception
	 */
	@Test
	public void client() throws Exception {
		// 1.获取连接
		Connection connection = ConnectionUtil.getConnection();
		// 2.获取通道
		Channel channel = connection.createChannel();
		// 3.循环发送-接收消息
		for (int i = 0; i < 32; i++) {
			// 4.构建请求消息的内容
			String message = Integer.toString(i);
			System.out.println(" [x] Requesting fib(" + message + ")");
			// 5.构建correlationId
			String correlationId = UUID.randomUUID().toString();
			// 6.构建接收响应的队列（通道自动创建非持久、自动删除的队列）
			String replyQueueName = channel.queueDeclare().getQueue();
			// 7.构建请求属性props
			AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().correlationId(correlationId).replyTo(replyQueueName).build();
			// 8.发送消息
			channel.basicPublish("", RPC_QUEUE_NAME, props, message.getBytes("UTF-8"));
			// 9.构建response对象
			final BlockingQueue<String> response = new ArrayBlockingQueue<String>(1);
			// 10.从通道中接收响应消息
			String ctag = channel.basicConsume(replyQueueName, true, new DefaultConsumer(channel) {
				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
					// 10.1响应必须匹配correlationId才会被接收
					if (properties.getCorrelationId().equals(correlationId)) {
						// 10.2响应消息放入response对象中
						response.offer(new String(body, "UTF-8"));
					}
				}
			});
			// 11.从response对象获取响应消息
			String res = response.take();
			// 12.销毁消费者
			channel.basicCancel(ctag);
			System.out.println(" [.] Got '" + res + "'");
		}
	}

	/**
	 * 服务端，响应时携带请求properties中的correlationId和replyQueueName（发送到哪个queue）
	 *	@ReturnType	void 
	 *	@Date	2018年8月22日	上午10:11:47
	 *  @Param  @throws Exception
	 */
	@Test
	public void server() throws Exception {
		// 1.获取连接
		Connection connection = ConnectionUtil.getConnection();
		// 2.获取通道
		Channel channel = connection.createChannel();
		// 3.声明队列
		channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
		// 4.清除队列中内容
		channel.queuePurge(RPC_QUEUE_NAME);
		// 5.队列每次只接收一条消息
		channel.basicQos(1);
		System.out.println(" [x] Server waiting RPC requests");
		// 6.定义消费者
		Consumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
				// 6.1根据correlationId构建replyProps
				AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder().correlationId(properties.getCorrelationId()).build();
				String responseMessage = "";
				try {
					// 6.2接收到请求中的消息
					String message = new String(body, "UTF-8");
					int n = Integer.parseInt(message);
					System.out.println(" [.] fib(" + message + ")");
					// 6.3构建响应消息
					responseMessage += fib(n);
				} catch (RuntimeException e) {
					System.out.println(" [.] " + e.toString());
				} finally {
					// 6.4从请求属性中获取响应队列名称（服务端发送响应消息到的队列，也是客户端接收响应消息的队列）
					String replyQueueName = properties.getReplyTo();
					// 6.5响应消息给客户端
					channel.basicPublish("", replyQueueName, replyProps, responseMessage.getBytes("UTF-8"));
					// 6.6反馈服务器（表示我收到了请求）
					channel.basicAck(envelope.getDeliveryTag(), false);
					// RabbitMq consumer worker thread notifies the RPC server
					// owner thread
					synchronized (this) {
						this.notify();
					}
				}
			}
		};
		// 7.消费者监听队列
		channel.basicConsume(RPC_QUEUE_NAME, false, consumer);
		// Wait and be prepared to consume the message from RPC client.
		while (true) {
			synchronized (consumer) {
				try {
					consumer.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 斐波那契数列
	 *	@ReturnType	int 
	 *	@Date	2018年8月22日	上午10:04:46
	 *  @Param  @param n		0 1 2 3 4 5 6  7  8  9  10 11 ...... 
	 *										||
	 *										||
	 *										\/ 		
	 *  @Param  @return			1,1,2,3,5,8,13,21,34,55,89,144......
	 */
	private static int fib(int n) {
		if (n == 0){
			return 0;
		}
		if (n == 1){
			return 1;
		}
		return fib(n - 1) + fib(n - 2);
	}

}
