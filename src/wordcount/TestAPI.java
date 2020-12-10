package wordcount;

import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestAPI {
	@Test
	public void testConnectNamenode() throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs:192.168.18.128:9000"),conf); 
		FileStatus [] ls =fs.listStatus(new Path("/"));
		for(FileStatus status:ls){
			System.out.println(status);
		}
	}
	
}
