package cloudproject.transcoding;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.CmdLineException;

public class TestCmdLine {
	
	public static String propertiesFileURL = null;
	public static String inputdataFolderURL = null;
	public static String outputdataFileURL = null;
	public static String sortalgorithm = null;
	public static boolean startupqueue = true;
	public static int jobNum = 0;
	public static int waitinglist_max = 0;
	public static int vmNum = 0;
	public static int frequency = 0;
	
	public static void testCmdLine(String[] args) {
		
		String test[] = {"-property","/Users/lxb200709/Documents/TransCloud/jarfiles/properties/config.properties", 
			             "-input",   "/Users/lxb200709/Documents/TransCloud/jarfiles/inputdata",
			             "-output",  "/Users/lxb200709/Documents/TransCloud/jarfiles/outputdata/test.txt", 
			             "-sortalgorithm", "SDF",
			             "-startupqueue",
			             "-videonum", "2",
			             "-vmqueue", "2",
			             "-vmNum", "0",  
			             "-vmfrequency", "0"};
		ParseCmdLine pcl = new ParseCmdLine();
		//new JCommander(pcl, test);
		
		CmdLineParser parser = new CmdLineParser(pcl);
		try {
            parser.parseArgument(test);
            //pcl.run(test);
            propertiesFileURL = pcl.getPropertiesFileURL();
    		System.out.println(propertiesFileURL);
    		
    		inputdataFolderURL = pcl.getInputdataFolderURL();
    	  	System.out.println(inputdataFolderURL);
    	  	
    	  	outputdataFileURL = pcl.getInputdataFolderURL();
    	  	System.out.println(outputdataFileURL);
    	  	
    	  	sortalgorithm = pcl.getSortAlgorithm();	 
    	  	System.out.println(sortalgorithm);

    	  	startupqueue = pcl.getStarupQueue();	
    	  	System.out.println(startupqueue);

    	  	jobNum = pcl.getVideoNum();	  	
    	  	System.out.println(jobNum);

    	  	waitinglist_max = pcl.getVmQueueSize();	  
    	  	System.out.println(waitinglist_max);

    	  	vmNum = pcl.getVmNum();
    	  	System.out.println(vmNum);

    	  	frequency = pcl.getVmFrequency();
    	  	System.out.println(frequency);


    	  	
    	  	
	    } catch (CmdLineException e) {
	        // handling of wrong arguments
	        System.err.println(e.getMessage());
	        parser.printUsage(System.err);
	    }

		
		

		
	}
}
