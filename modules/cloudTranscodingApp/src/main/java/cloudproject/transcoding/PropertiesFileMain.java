package cloudproject.transcoding;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

public class PropertiesFileMain {
	public static void main(String[] args){
		Properties prop = new Properties();
		OutputStream output = null;

	
		try {

			output = new FileOutputStream("/Users/lxb200709/Documents/TransCloud/cloudsim/modules/cloudTranscodingApp/config.properties");
			
			/**
			 * configuration properties in datacenter
			 */
			//vm local waiting queue size
			prop.setProperty("waitinglist_max", "2");
			
			
			
	
			// save properties to project root folder
			prop.store(output, null);
			
			
			/*InputStream input = new FileInputStream("/Users/lxb200709/Documents/TransCloud/cloudsim/modules/cloudsim-impl/config.properties");
			prop.load(input);
			for(String key : prop.stringPropertyNames()){
				String value = prop.getProperty(key);
				int val = Integer.valueOf(value);
				System.out.println( key + ": " + val);
			}*/
			
	
		} catch (IOException io) {
			io.printStackTrace();
		} finally {
			if (output != null) {
				try {
					output.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
	
		}
   }
}
