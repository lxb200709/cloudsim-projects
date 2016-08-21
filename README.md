#On-Demand Video Transcoding Using Cloud Services#
The architecture for on-demand video transcoding includes includes six main components, namely ```video splitter```, ```task (i.e., GOP) scheduler```, ```transcoding virtual machines (VM)```, ```elasticity manager```, ```video merger```, and ```caching policy```.

```video splitter``` splits a video into GOPs, which can be transcoded independently. The code for video segmentation can be downloaded [here](https://github.com/lxb200709/videotranscoding_gop). Also, the benchmark videos can be downloaded [here](https://goo.gl/TE5iJ5).

The main contributions of this project are a QoS-aware scheduling method for ```task (i.e., GOP) scheduler``` and a dynamic resource provisioning policy for ```elasticity manager```.

<img src="architecture.png" width="400">

##How to use it in Eclipse

####Step1: Download and import project
```bash
File -> Import -> Maven -> Existing Maven Projects -> browse the location of cloudsim-projects/module
```
####Step2: Download dependent jars through Maven
```bash
Right click module -> Maven -> Update Projects
Right click module -> Run As -> Maven build ... -> Goals ("clean install") -> Run
```

##Project structure

This project includes four module:
```bash
  * cloudsim
  * cloudsim-example
  * cloudsim-impl
  * cloudTranscodingApp
```
```cloudsim``` and ```cloudsim-example``` module are from original cloudsim package, more information can be found on the CloudSim's web site.

```cloudsim-impl``` module mainly implements broker and datacenter for scheduling, it contains class extends class from cloudsim (e.g. TranscodingBroker extends DatacenterBroker, VideoSegment extends Cloudlet...)

```cloudTranscodingApp``` includes the main function where the whole simulation starts. The system reading video requests through here. Resource provisioning is also implemented in this module.

How to test

Define command lines.
```java
String[] args = {"-property", "/your/directory/to/put/config.properties",    //location to store property file
                 "-input", "/your/directory/to/put/inputdata",               //location of inputdata
                 "-output", "/your/directory/to/put/output.txt",             //location for outputdata
                 "-sortalgorithm", "SDF",          //sorting algorithm
                 "-startupqueue",                  //whether includes startup queue or not
                 "-stqprediction",                 //whether include startup queue prediction or not
                 "-videonum", "500",               //video request number
                 "-vmqueue", "1",                  //vm local queue length
                 "-vmNum", "0",                    //vm number, "0" means dynamic
                 "-vmfrequency", "10000",          //vm provisioning frequency
                 "-goplength", "AVERAGE",          //gop length
                 "-upthreshold", "0.10",           //provisioning upper threadshold
                 "-lowthreshold", "0.05",          //provisioning lower threadshold
                 "-testPeriod", "1200000",         //Test period
                 "-rentingTime", "100000",         //vm renting time
                 "-seedshift", "2"};
 ```
##Download

The downloaded package contains all the source code, examples, jars, and API html files.

##Publications

* **Xiangbo Li**, Mohsen Amini Salehi, Magdi Bayoumi, Rajkumar Buyya, [CVSS: A Cost-Efficient and QoS-Aware Video Streaming Using Cloud Services](http://hpcclab.org/paperPdf/ccgrid16/CloudTranscodingconf.pdf), in Proceedings of 16th ACM/IEEE International Conference on Cluster Cloud and Grid Computing (CCGrid ’16), Columbia, May 2016.
* **Xiangbo Li**, Mohsen Amini Salehi, Magdi Bayoumi, [High Performance On-Demand Video Transcoding Using Cloud Services](http://hpcclab.org/paperPdf/ccgrid16/CloudTransSymp.pdf), in Proceedings of the 16th ACM/IEEE International Conference on Cluster Cloud and Grid Computing (CCGrid ’16), Columbia, May 2016 (Doctoral symposium)
* **Xiangbo Li**, Mohsen Amini Salehi, Magdi Bayoumi, Cloud-Based Video Streaming for Energy- and Compute-Limited Thin Clients, Presented in Stream2015 Workshop at Indiana University, Indianapolis, USA, Oct. 2015.

##Licence

Source code can be found on [github](https://github.com/lxb200709/cloudsim-projects).

Developed by [Xiangbo Li](https://www.linkedin.com/in/xiangbo-li-2893582a)
