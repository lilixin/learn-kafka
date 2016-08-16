package kafka;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import top.lilixin.TopProducer;

@ContextConfiguration({"classpath:applicationContext.xml"})
@RunWith(SpringJUnit4ClassRunner.class)
public class TestCase {
	
	@Autowired
	private TopProducer topProducer;
	
	
	private String topic = "lilixin";

	@Test
	public void testCase(){
		System.out.println("##############################");
		topProducer.send(topic,"this ia a kafka test msg");
		System.out.println("##############################");
	}
	
}
