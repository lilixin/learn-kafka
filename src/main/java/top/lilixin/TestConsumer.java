package top.lilixin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Project: kafka
 * @Author: lilixin
 * @Date: 2016年8月16日
 * @Copyright: 2016 www.lilixin.top Inc. All rights reserved.
 */
public class TestConsumer implements TopConsumer {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void dealMsg(String msg) {
		logger.info("***********************");
		logger.info("要消费的消息={}", msg);
		logger.info("***********************");
	}

}
