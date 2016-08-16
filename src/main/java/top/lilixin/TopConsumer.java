/*
 * @项目名称: kafka
 * @文件名称: TopConsumer.java
 * @Date: 2016-8-16
 * @Copyright: 2016 www.lilixin.top Inc. All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的
 */
package top.lilixin;


/**
 * @Project: kafka
 * @Author: lilixin
 * @Date: 2016年8月16日
 * @Copyright: 2016 www.lilixin.top Inc. All rights reserved.
 */
public interface TopConsumer {
	
	public void dealMsg(String strings);
}
