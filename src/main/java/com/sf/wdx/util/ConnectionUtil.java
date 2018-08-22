package com.sf.wdx.util;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;

/**
 * 描述：RabbitMQ连接工具类
 * @author 80002888
 * @date   2018年8月20日
 */
public class ConnectionUtil {

	/**
	 * 获取rabbitmq的连接
	 *	@ReturnType	Connection 
	 *	@Date	2018年8月20日	下午3:00:31
	 *  @Param  @return
	 *  @Param  @throws Exception
	 */
    public static Connection getConnection() throws Exception {
        // 1.获取连接工厂
        ConnectionFactory factory = new ConnectionFactory();
        // 2.设置rabbitmq服务器地址
        factory.setHost("localhost");
        // 3.设置端口
        factory.setPort(5672);
        // 4.设置账号信息，用户名、密码、vhost
        factory.setVirtualHost("/wdx");
        factory.setUsername("wdx");
        factory.setPassword("wdx");
        // 5.获取连接
        Connection connection = factory.newConnection();
        return connection;
    }

}
