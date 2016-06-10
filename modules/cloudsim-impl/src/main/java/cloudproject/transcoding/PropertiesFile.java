package cloudproject.transcoding;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

public class PropertiesFile {
	
	//public void setProperties(){
	public static void main(String[] args){
		Properties prop = new Properties();
		OutputStream output = null;

	
		try {

			output = new FileOutputStream("/Users/lxb200709/Documents/TransCloud/cloudsim/modules/cloudsim-impl/config.properties");
			
			/**
			 * configuration properties in main
			 */
			//video job number, value i means i+1 videos
			prop.setProperty("periodEventNum", "2");
			
			//check vm provision frequence
			prop.setProperty("periodicDelay", "20000");

			// save properties to project root folder
			prop.store(output, null);
			
	
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
