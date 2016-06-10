package cloudproject.transcoding;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

public class printOutputFile {
	
	public void printOutToFile(String filename){
	    	
	    	try{
	    		PrintStream myconsole = new PrintStream(new File("/home/yamini/Documents/resources/consoleLog"+filename+".txt"));
	    		System.setOut(myconsole);
	    		
	    	}catch(FileNotFoundException e){
	    		 e.printStackTrace();
	    	}
	
	}

}

