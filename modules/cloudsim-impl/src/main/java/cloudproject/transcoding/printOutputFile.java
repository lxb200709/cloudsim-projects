package cloudproject.transcoding;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

public class printOutputFile {
	
	public void printOutToFile(String filename){
	    	
	    	try{
	    		PrintStream myconsole = new PrintStream(new File("/Users/lxb200709/Documents/TransCloud/outputPrint/"+filename+".txt"));
	    		System.setOut(myconsole);
	    		
	    	}catch(FileNotFoundException e){
	    		 e.printStackTrace();
	    	}
	
	}

}

