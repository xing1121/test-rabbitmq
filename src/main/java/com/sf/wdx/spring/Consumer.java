package com.sf.wdx.spring;

/***
 * 描述：消费者
 * @author 80002888
 * @date   2018年8月22日
 */
public class Consumer {

    /**
     * 监听消息的方法
     *	@ReturnType	void 
     *	@Date	2018年8月22日	上午11:13:58
     *  @Param  @param message
     */
    public void listen(String message) {
        System.out.println("消费者收到消息： " + message);
    }
}