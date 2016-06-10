package cloudproject.transcoding;


import org.kohsuke.args4j.Option;


public class ParseCmdLine {
	
	@Option(name = "-property", usage = "property file url")
    private String propertiesFileURL =  null;

	@Option(name = "-input", usage = "input data folder url")
    private String inputdataFolderURL = null;

	@Option(name = "-output", usage = "output file url")
    private String outputdataFileURL = null;
    
	@Option(name = "-sortalgorithm", usage = "sorting algorithm")
    private String sortalgorithm = null;
    
	@Option(name = "-startupqueue", usage = "with start up queue or not")
    private boolean startupqueue;
	
	@Option(name = "-stqprediction", usage = "use startup queue prediction or not")
    private boolean stqprediction;

	@Option(name = "-videonum", usage = "video number")
    private Integer jobNum = 0;
    
	@Option(name = "-vmqueue", usage = "vm local queue size")
    private Integer waitinglist_max = 0;
    
	@Option(name = "-vmNum", usage = "vm number")
    private Integer vmNum = 0;
    
	@Option(name = "-vmfrequency", usage = "video number")
    private Integer frequency = 0;
	
	@Option(name = "-goplength", usage = "estimated gop length")
    private String gopLength = null;
	
	@Option(name = "-upthreshold", usage = "deadline miss rate upthredshold")
    private Double upthredshold = 0.0;
	
	@Option(name = "-lowthreshold", usage = "deadline miss rate lowthredshold")
    private Double lowthredshold = 0.0;
	
	@Option(name = "-testPeriod", usage = "test time period")
    private Double testPeriod = 0.0;
	
	@Option(name = "-rentingTime", usage = "vm renting time")
    private Long rentingTime = (long) 0;
	
	@Option(name = "-seedshift", usage = "change seed")
    private Integer seedshift = 0;
	
	
	public Boolean getStqPrediction(){
		return stqprediction;
	}
	
	public Double getTestPeriod(){
		return testPeriod;
	}
	
	public Long getRentingTime(){
		return rentingTime;
	}
	
	public Double getUpThredshold() {
		return upthredshold;
	}
	
	public Double getLowThreshold() {
		return lowthredshold;
	}

    public String getEstimatedGopLength(){
    	return gopLength;
    }
    
    public String getPropertiesFileURL() {
		return propertiesFileURL;
	} 
    
    public String getInputdataFolderURL(){
    	return inputdataFolderURL;
    }
    
    public String getOutputdataFileURL(){
    	return outputdataFileURL;
    }
    
    public String getSortAlgorithm(){
    	return sortalgorithm;
    }
    
    public boolean getStarupQueue() {
    	return startupqueue;
    }
    
    public int getVideoNum(){
    	return jobNum;
    }
    
    public int getVmQueueSize(){
    	return waitinglist_max;
    }
    
    public int getVmNum(){
    	return vmNum;
    }
    
    public int getVmFrequency(){
    	return frequency;
    }
    
    public int getSeedShift(){
    	return seedshift;
    }
    
    /**
	 * If you want to get the args-Array from the command line
	 * use the signature <tt>run(String[] args)</tt>. But then there must 
	 * not be a run() because that is executed prior to this.
	 * @param args The arguments as specified on the command line
	 */
	public void run(String[] args) {
		System.out.println("SampleStarter.run(String[])");
		System.out.println("- args.length: " + args.length);
		for (String arg : args) System.out.println("  - " + arg);
		//System.out.println(this);
	}

    
}
