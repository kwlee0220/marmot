package marmot.mapreduce;

import org.apache.hadoop.mapreduce.Job;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface MapReduceJobConfigurer {
	public void configure(Job job);
	
	public static MapReduceJobConfigurer configure(Object obj, Job job) {
		if ( obj != null && obj instanceof MapReduceJobConfigurer ) {
			MapReduceJobConfigurer configurer = (MapReduceJobConfigurer)obj;
			configurer.configure(job);
			
			return configurer;
		}
		else {
			return null;
		}
	}
}
