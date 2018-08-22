package com.sf.wdx.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 描述：时间转换工具类
 * @author 80002888
 * @date   2018年8月20日
 */
public class DateUtils {

	private static Lock lock = new ReentrantLock();	
	
	/**
	 * 时间转字符串
	 *	@ReturnType	String 
	 *	@Date	2018年8月20日	上午11:17:06
	 *  @Param  @param date
	 *  @Param  @param pattern
	 *  @Param  @return
	 */
	public static String date2Str(Date date, String pattern){
		lock.lock();
		try {
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
			return simpleDateFormat.format(date);
		} finally {
			lock.unlock();
		}
	}
	
	/**
	 * 字符串转时间
	 *	@ReturnType	Date 
	 *	@Date	2018年8月20日	上午11:17:16
	 *  @Param  @param dateStr
	 *  @Param  @param pattern
	 *  @Param  @return
	 *  @Param  @throws Exception
	 */
	public static Date str2Date(String dateStr, String pattern) throws Exception{
		lock.lock();
		try {
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
			return simpleDateFormat.parse(dateStr);
		} finally {
			lock.unlock();
		}
	}
	
}
