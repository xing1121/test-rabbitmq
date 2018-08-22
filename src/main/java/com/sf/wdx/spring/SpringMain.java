package com.sf.wdx.spring;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class SpringMain {
	
	public static void main(final String... args) throws Exception {
		// 启动Spring
		AbstractApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:spring/spring-rabbitmq.xml");

		// 获取RabbitMQ模板
		RabbitTemplate template = ctx.getBean(RabbitTemplate.class);

		// 发送消息
		String message = "Hello, world!";
		System.out.println("生产者发送消息：" + message);
		template.convertAndSend(message);

		// 休眠1秒
		Thread.sleep(1000);

		// 关闭容器
		ctx.close();
	}
	
}
