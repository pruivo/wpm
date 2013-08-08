/*
 * CINI, Consorzio Interuniversitario Nazionale per l'Informatica
 * Copyright 2013 CINI and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
 
package eu.cloudtm.wpm.logService;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.rmi.ssl.SslRMIClientSocketFactory;
import javax.rmi.ssl.SslRMIServerSocketFactory;

import org.apache.log4j.Logger;
import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

import eu.cloudtm.wpm.consumer.AckConsumer;
import eu.cloudtm.wpm.logService.remote.observables.*;

import eu.cloudtm.wpm.logService.remote.events.*;

import eu.cloudtm.wpm.logService.remote.listeners.*;

import eu.cloudtm.wpm.logService.remote.publisher.*;

import eu.cloudtm.wpm.parser.*;


/**
* @author Roberto Palmieri
* @author Sebastiano Peluso
*/
public class LogServiceAnalyzer implements Runnable{
	
	private final static Logger log = Logger.getLogger(LogServiceAnalyzer.class);
	private final static boolean INFO = log.isInfoEnabled();
	private final static boolean DEBUG = log.isDebugEnabled();
	
	private static int RMI_REGISTRY_PORT = 1099;
	
	static final int filesize = 2048;
	private Cache<String,String> cache;
	private String cacheName;
	private String infinispanConfigFile;
	private long timeout;
	private boolean enableInfinispan;
	
	private boolean enableListeners;
	
	private WPMObservableImpl observable;
	
	private HashMap<String, Long> jmxNodes;
	
	private long numJmxNodes = 0;
	
	private long numCheckJmxNodes = 0;
	
	private StatsSubscriptionEntry csvFileStaticLocalSubscription;
	
	private HashMap<String, Aggregation> aggregationTypes;

	
	public LogServiceAnalyzer() throws RemoteException{
		loadParametersFromRegistry();
		loadAggregationTypesFromRegistry();
		
		if(enableInfinispan){
			GlobalConfiguration gc = GlobalConfiguration.getClusteredDefault();

			gc.setClusterName("LogServiceConnection");
			Configuration c = new Configuration();
			c.setCacheMode(Configuration.CacheMode.REPL_SYNC);
			c.setExpirationLifespan(-1);
			c.setExpirationMaxIdle(-1);
			EmbeddedCacheManager cm = new DefaultCacheManager(gc, c);
			this.cache = cm.getCache();

		}
		if(INFO)
			log.info("Running Log Service Analyzer Thread!!");
		
		//TransactionManager tm = cache.getAdvancedCache().getTransactionManager();+
		
		
		Registry registry=LocateRegistry.createRegistry(RMI_REGISTRY_PORT);
		
		this.observable = new WPMObservableImpl();
		
		WPMObservable stub_observable= this.observable;
		
		registry.rebind("WPMObservable", stub_observable);
		
		
		
		this.jmxNodes = new HashMap<String, Long>();
		
		
		
		Thread analyzer = new Thread(this,"Log Service Analyzer");
		analyzer.start();
	}

	@Override
	public void run() {
		while(true){
			try {
				Thread.sleep(timeout);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			try {
				if(INFO){
					if(cache!=null)
						log.info("Dataitem in cache: "+cache.size());
					log.info("Running Log Service Analyzer Thread!!");
				}
				File active_folder = new File("log/ls_processed");
				if(active_folder.isDirectory()){
					for(File activeFile : active_folder.listFiles()){
						if(!activeFile.getName().endsWith(".zip"))
							continue;
						try {
					        FileInputStream is = new FileInputStream(activeFile);
						    ZipInputStream zis = new ZipInputStream(new BufferedInputStream(is));
						    ZipEntry entry = null;
						    FileOutputStream fos = null;
						    String nameFileToStore = "";
						    if((entry = zis.getNextEntry()) != null){
						    	byte [] logFileByteArray  = new byte [filesize];
						    	int count = 0;
						    	nameFileToStore = "log/ls_worked/"+entry.getName();
						    	fos = new FileOutputStream(nameFileToStore);
						    	BufferedOutputStream dest = new BufferedOutputStream(fos, filesize);
						    	if(INFO)
						    		log.info("Extracting: " +entry.getName());
								while ((count = zis.read(logFileByteArray, 0, filesize)) != -1) {
									dest.write(logFileByteArray, 0, count);
								}
								dest.flush();
						    }
						    fos.close();
						    zis.close();
						    if(INFO)
						    	log.info("File decompressed stored");
						    //Create ack file
						    File ackFile = new File("log/ls_worked/"+activeFile.getName().substring(0,activeFile.getName().lastIndexOf(".zip"))+".ack");
				            if(!ackFile.createNewFile()){
				            	if(INFO)
				            		log.info("Error while creating ack file");
				            	
				            }
				            
				            
				            if(INFO)
				            	log.info("Ack file stored: "+ackFile.getPath());
				            if(enableInfinispan || enableListeners){
				            	//timestamp nomeMetrica:valore;nomeMetrica:valore;nomeMetrica:valore
				            	String strLine = "";
				            	FileInputStream fstream = new FileInputStream(nameFileToStore);
				            	DataInputStream in = new DataInputStream(fstream);
				            	BufferedReader br = new BufferedReader(new InputStreamReader(in));
				            	
				            	
				            	boolean membersChanged = false;
				            	
				            	while ((strLine = br.readLine()) != null){
				            		if(INFO)
				            			log.info("Data received by LogService before Parsing:\n"+strLine);
				            		Measurement mis = WPMParser.parseLine(strLine);
				            		if(mis == null)
				            			continue;


				            		if(enableListeners){
				            			
				            			
				            			membersChanged = checkJmxMembers(mis);
				            			
				            			
				            			if(membersChanged){
					            			
					            			
					            			int size = this.jmxNodes.size();
					            			Set<String> set = this.jmxNodes.keySet();
					            			
					            			String[] addresses = new String[size];
					            			
					            			int i = 0;
					            			
					            			System.out.println("--- View Change ---");
					            			for(String key: set){
					            				addresses[i] = key;
					            				System.out.println(key);
					            				i++;
					            				
					            			}
					            			System.out.println("-------------------");
					            			
					            			this.csvFileStaticLocalSubscription = new StatsSubscriptionEntry(null, addresses, null);//No handle and no remote listener here
					            			
					            			
					            			PublishViewChangeEvent event = new PublishViewChangeEvent(addresses);
					            			
					            			
					            			Iterator<ViewSubscriptionEntry> itr = this.observable.getViewIterator();
					            			
					            			while(itr.hasNext()){
					            				
					            				ViewSubscriptionEntry current = itr.next();
					            				
					            				PublisherViewThread pt = new PublisherViewThread(current, event, this.observable);
					            				
					            				pt.start();
					            				
					            			}
					            			
					            			
					            		}
				            			
				            			
				            		
				            			
				            			String ip = mis.getIp();
				            			int resourceType = mis.getResourceType().ordinal();
				            			
				            			//Add measure to each dynamic subscription
				            			Iterator<StatsSubscriptionEntry> itr = this.observable.getStatsIterator();
				            			
				            			while(itr.hasNext()){
				            				
				            				
				            				itr.next().addMeasurement(mis);
				            				
				            			}
				            			
				            			//Add measure to the static subscription
				            			if(this.csvFileStaticLocalSubscription != null){
				            				this.csvFileStaticLocalSubscription.addMeasurement(mis);
				            			}
				            			
				            		}
				            		

				            		if(enableInfinispan){
				            			int num_of_res_ind = mis.getNumberOfResIndex();
				            			String mis_spec = "";
				            			for(int i=0;i<num_of_res_ind;i++){
				            				//String identification_key = mis.getComponent_ID()+":"+mis.getResourceType()+":"+i;
				            				String identification_key = mis.getIp()+":";
				            				if(mis.getResourceType() == ResourceType.JMX){
				            					identification_key += "Infinispan Cache ( CloudTM )"+":"+i;
				            				}else{
				            					identification_key += mis.getResourceType()+":"+i;
				            				}

				            				String payload = mis.getTimestamp()+";"+mis.getAttributesForInfinispan(i);
				            				cache.put(identification_key,payload);
				            				String date_format = "";
				            				try{
				            					SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm");
				            					date_format = sdf.format(new Date(mis.getTimestamp()));
				            				}catch(Exception e){}
				            				if(INFO){
				            					log.info("Put done! Id: "+identification_key+" timestap: "+date_format);
				            					log.info("Put done! Value: "+payload);
				            				}
				            				if(mis.getResourceType() == ResourceType.DISK){
				            					String [] att_vect = mis.getAttributesForInfinispan(i).split(";");
				            					for(int k=0;k<att_vect.length;k++){
				            						if(att_vect[k] != null && att_vect[k].startsWith("fileSystemUsage.mounting_point")){
				            							mis_spec += i+"="+att_vect[k].substring(att_vect[k].indexOf("fileSystemUsage.mounting_point$$")+(new String("fileSystemUsage.mounting_point$$").length()))+";";
				            						}
				            					}
				            				}
				            				if(mis.getResourceType() == ResourceType.NETWORK){
				            					mis_spec += i+"=en"+i+";";
				            				}
				            			}

				            			String platforms = cache.get("WPMPlatforms");
				            			if(platforms == null){
				            				cache.put("WPMPlatforms", mis.getIp());
				            			}else{
				            				if(!platforms.contains(mis.getIp())){
				            					platforms += ";"+mis.getIp();
				            					cache.put("WPMPlatforms", platforms);
				            				}
				            			}
				            			String plats = cache.get("WPMPlatforms");
				            			if(INFO)
				            				log.info("Platforms: "+plats);
				            			switch(mis.getResourceType()){
				            			case CPU 		: cache.put("WPMPlatform_NUM_CPUS:"+mis.getIp(), ""+num_of_res_ind);break;
				            			case DISK 		: cache.put("WPMPlatfom_MOUNTING_PTS:"+mis.getIp(), ""+mis_spec);break;
				            			case NETWORK 	: cache.put("WPMPlatfom_NETWORKS:"+mis.getIp(), ""+mis_spec);break;
				            			case JMX		: cache.put("WPMPlatfom_CACHES:"+mis.getIp(), "CloudTM");break;
				            			default : if(mis.getResourceType() != ResourceType.MEMORY){ throw new RuntimeException("Error!");};
				            			}
				            			if(INFO){
				            				log.info("WPMPlatform_NUM_CPUS:"+mis.getIp()+": "+cache.get("WPMPlatform_NUM_CPUS:"+mis.getIp()));
				            				log.info("WPMPlatfom_MOUNTING_PTS:"+mis.getIp()+": "+cache.get("WPMPlatfom_MOUNTING_PTS:"+mis.getIp()));
				            				log.info("WPMPlatfom_NETWORKS:"+mis.getIp()+": "+cache.get("WPMPlatfom_NETWORKS:"+mis.getIp()));
				            				log.info("WPMPlatfom_CACHES:"+mis.getIp()+": "+cache.get("WPMPlatfom_CACHES:"+mis.getIp()));
				            			}
				            		}
				            	}

				            	if(enableListeners){
				            		// Try to publish here
				            		// First try to publish for the static subscription (it produces the cvs files!!!)

				            		PublishStatsEventInternal statsToPublish = null;

				            		if(this.csvFileStaticLocalSubscription != null){
				            			statsToPublish = this.csvFileStaticLocalSubscription.computePublishStatsEventInternal();
				            		}

				            		// produce CSV here. This method returns the aggregated stats if any.
				            		AggregatedPublishAttributes aggregations = produceCSV(statsToPublish, System.currentTimeMillis());

				            		// Then try to publish for the dynamic subscriptions

                                    Iterator<StatsSubscriptionEntry> itr = this.observable.getStatsIterator();

                                    int size = this.observable.numStatsSubscriptions();

                                    List<PublishStatsEventInternal> toPublish = new ArrayList<PublishStatsEventInternal>(size);

                                    while (itr.hasNext()) {
                                        PublishStatsEventInternal publishStatsEventInternal = itr.next().computePublishStatsEventInternal();

                                        if (publishStatsEventInternal != null) {
                                            toPublish.add(publishStatsEventInternal);
                                            publishStatsEventInternal.addAggregations(aggregations);
                                        }

                                    }

                                    for (PublishStatsEventInternal publishStatsEventInternal : toPublish) {
                                        PublisherStatsThread pt = new PublisherStatsThread(publishStatsEventInternal, this.observable);
                                        pt.start();
                                    }
                                }

				            	if(INFO){
				            		if(cache != null)
				            			log.info("Dataitem in cache: "+cache.size());
				            	}
				            	if(!activeFile.delete()){
							    	if(INFO)
							    		log.info("ZIP file not delted!! "+activeFile);
							    }	
							    //System.out.println("Deleted file: "+activeFile.getName());
				            }
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	private AggregatedPublishAttributes produceCSV(PublishStatsEventInternal pei, long timestamp){

		if(pei != null){
			PublishStatisticsEvent pse = pei.getPerSubscriptionEvent();

			List<PublishAttribute[]> cpuStats = new LinkedList<PublishAttribute[]>();
			List<PublishAttribute[]> memoryStats = new LinkedList<PublishAttribute[]>();
			List<PublishAttribute[]> networkStats = new LinkedList<PublishAttribute[]>();
			List<PublishAttribute[]> diskStats = new LinkedList<PublishAttribute[]>();
			List<PublishAttribute[]> jmxStats = new LinkedList<PublishAttribute[]>();
            List<PublishAttribute[]> fenixStats = new LinkedList<PublishAttribute[]>();


			if(pse != null){
				Set<String> IPs = pse.getIps();
				ResourceType currentResourceType;

				int numResources;
				PublishAttribute[][] allAttr;
				for(String currentIP: IPs){
					//This is one for each machine!


					//CPU
					currentResourceType = ResourceType.CPU;
					//numResources = pse.getNumResources(currentResourceType, currentIP);

					//for(int i = 0; i<numResources; i++){
						
						//cpuStats.add(produceCSVFor(pse, currentIP, currentResourceType, i));

					//}
					
					allAttr = produceCSVFor(pse, currentIP, currentResourceType, timestamp);
					if(allAttr != null){
						for(int i = 0; i<allAttr.length; i++){
							
							cpuStats.add(allAttr[i]);

						}
					}


					//Memory
					currentResourceType = ResourceType.MEMORY;
					
					//numResources = pse.getNumResources(currentResourceType, currentIP);
					//for(int i = 0; i<numResources; i++){

					//	memoryStats.add(produceCSVFor(pse, currentIP, currentResourceType, i));

					//}
					
					allAttr = produceCSVFor(pse, currentIP, currentResourceType, timestamp);
					if(allAttr != null){
						for(int i = 0; i<allAttr.length; i++){
							
							memoryStats.add(allAttr[i]);

						}
					}


					//Disk
					currentResourceType = ResourceType.DISK;
					
					//numResources = pse.getNumResources(currentResourceType, currentIP);
					//for(int i = 0; i<numResources; i++){

					//	diskStats.add(produceCSVFor(pse, currentIP, currentResourceType, i));

					//}
					allAttr = produceCSVFor(pse, currentIP, currentResourceType, timestamp);
					if(allAttr != null){
						for(int i = 0; i<allAttr.length; i++){
							
							diskStats.add(allAttr[i]);

						}
					}

					//Network
					currentResourceType = ResourceType.NETWORK;
					//numResources = pse.getNumResources(currentResourceType, currentIP);
					//for(int i = 0; i<numResources; i++){

					//	networkStats.add(produceCSVFor(pse, currentIP, currentResourceType, i));

					//}
					allAttr = produceCSVFor(pse, currentIP, currentResourceType, timestamp);
					if(allAttr != null){
						for(int i = 0; i<allAttr.length; i++){
							
							networkStats.add(allAttr[i]);

						}
					}
					

					//JMX
					currentResourceType = ResourceType.JMX;
					
					//numResources = pse.getNumResources(currentResourceType, currentIP);
					//for(int i = 0; i<numResources; i++){

					//	jmxStats.add(produceCSVFor(pse, currentIP, currentResourceType, i));

					//}
					
					allAttr = produceCSVFor(pse, currentIP, currentResourceType, timestamp);
					if(allAttr != null){
						for(int i = 0; i<allAttr.length; i++){
							
							jmxStats.add(allAttr[i]);

						}
					}

                    //FENIX
                    currentResourceType = ResourceType.FENIX;
                    allAttr = produceCSVFor(pse, currentIP, currentResourceType, timestamp);

                    if(allAttr != null){
                        Collections.addAll(fenixStats, allAttr);
                    }
				}
				
				PublishAttribute[] aggregatedCPU = produceAggregationFor(cpuStats);
				PublishAttribute[] aggregatedMemory = produceAggregationFor(memoryStats);
				PublishAttribute[] aggregatedDisk = produceAggregationFor(diskStats);
				PublishAttribute[] aggregatedNetwork = produceAggregationFor(networkStats);
				PublishAttribute[] aggregatedJmx = produceAggregationFor(jmxStats);
                PublishAttribute[] aggregatedFenix = produceAggregationFor(fenixStats);
				
				AggregatedPublishAttributes agg = new AggregatedPublishAttributes(timestamp);
				agg.add(ResourceType.CPU, aggregatedCPU);
				agg.add(ResourceType.MEMORY, aggregatedMemory);
				agg.add(ResourceType.DISK, aggregatedDisk);
				agg.add(ResourceType.NETWORK, aggregatedNetwork);
				agg.add(ResourceType.JMX, aggregatedJmx);
                agg.add(ResourceType.FENIX, aggregatedFenix);
				
				
				produceAggregatedCSVFor(aggregatedCPU, aggregatedMemory, aggregatedDisk, aggregatedNetwork,
                        aggregatedJmx, aggregatedFenix, timestamp);
				
				//produceAggregatedCSVFor(ResourceType.CPU, cpuStats);
				//produceAggregatedCSVFor(ResourceType.MEMORY, memoryStats);
				//produceAggregatedCSVFor(ResourceType.DISK, diskStats);
				//produceAggregatedCSVFor(ResourceType.NETWORK, networkStats);
				//produceAggregatedCSVFor(ResourceType.JMX, jmxStats);
					
				return agg;
			}

		}
		
		return null;
	}
	
	
	private void produceAggregatedCSVFor(PublishAttribute[] aggregatedCPU, PublishAttribute[] aggregatedMemory,
                                         PublishAttribute[] aggregatedDisk, PublishAttribute[] aggregatedNetwork,
                                         PublishAttribute[] aggregatedJmx, PublishAttribute[] aggregatedfenix, long timestamp){
		
			PublishAttribute[] current;
			
			FileOutputStream fos = null;
			PrintStream out = null;
			try {
				boolean exists = false;
				File fileFolder = new File("log/csv/cluster");
				
				exists = fileFolder.exists();
				if(!exists){
					fileFolder.mkdirs();
				}
				
				File file = new File(fileFolder,"cluster.csv");
				
				exists = file.exists();
				
				fos = new FileOutputStream(file, true);
				out = new PrintStream(fos);
				String line;
				String header;
				if(out != null){
					line=""+timestamp+",";
					header="Timestamp,";
					
					current = aggregatedCPU;
					if(current != null){
						for(int i = 0; i<current.length; i++){

							if(current[i] != null){
								header+=""+current[i].getName();
								line+=""+current[i].getValue();

								header+=",";
								line+=",";

								
							}	

						}

					}
					
					current = aggregatedMemory;
					if(current != null){
						for(int i = 0; i<current.length; i++){

							if(current[i] != null){
								header+=""+current[i].getName();
								line+=""+current[i].getValue();

								header+=",";
								line+=",";

								
							}	

						}

					}
					
					current = aggregatedDisk;
					if(current != null){
						for(int i = 0; i<current.length; i++){

							if(current[i] != null){
								header+=""+current[i].getName();
								line+=""+current[i].getValue();

								header+=",";
								line+=",";

								
							}	

						}

					}
					
					current = aggregatedNetwork;
					if(current != null){
						for(int i = 0; i<current.length; i++){

							if(current[i] != null){
								header+=""+current[i].getName();
								line+=""+current[i].getValue();

								header+=",";
								line+=",";

								
							}	

						}

					}
					
					current = aggregatedJmx;
					if(current != null){
						for(int i = 0; i<current.length; i++){

							if(current[i] != null){
								header+=""+current[i].getName();
								line+=""+current[i].getValue();
                                header+=",";
                                line+=",";
							}	

						}

					}

                    current = aggregatedfenix;
                    if(current != null){
                        for(int i = 0; i<current.length; i++){
                            if(current[i] != null){
                                header+=""+current[i].getName();
                                line+=""+current[i].getValue();
                                if(i!=current.length-1){
                                    header+=",";
                                    line+=",";
                                }
                            }
                        }
                    }

					if(!exists){
						out.println(header);
					}
					out.println(line);
					out.flush();
					
					
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			finally{
				if(out != null){
					out.close();
				}
				
				if(fos != null){
					try {
						fos.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			
		}
		
	
	private PublishAttribute[] produceAggregationFor(List<PublishAttribute[]> list){
		
        PublishAttribute[] result = null;
		
		int numSeenSamples = 0;
		
		if(list!=null){
			
			for(PublishAttribute[] current: list){
				
				if(result == null){
					
					result = new PublishAttribute[current.length];
					
					for(int i = 0; i < current.length; i++){
						
						
						if(getAggregationType(current[i]) == Aggregation.NO){
							
							result[i] = null;
							
						}
						else{
							result[i] = new PublishAttribute(current[i].getResourceType(), current[i].getResourceIndex(), current[i].getAttributeIndex(), current[i].getName(), current[i].getValue());
						}
						
						
					}
					
				}
				else{
					
					for(int i = 0; i < current.length; i++){
						
						aggregate(result[i], current[i], getAggregationType(current[i]), numSeenSamples);
						
						
					}
					
					
					
				}
				
				numSeenSamples++;
				
			}
		}	
		return result;	
	}
	/*
	private void produceAggregatedCSVFor(ResourceType res, List<PublishAttribute[]> list){
		
		PublishAttribute[] result = null;
		
		int numSeenSamples = 0;
		
		if(list!=null){
			
			for(PublishAttribute[] current: list){
				
				if(result == null){
					
					result = new PublishAttribute[current.length];
					
					for(int i = 0; i < current.length; i++){
						
						if(getAggregationType(current[i]) == Aggregation.NO){
							
							result[i] = null;
							
						}
						else{
							result[i] = new PublishAttribute(current[i].getResourceType(), current[i].getResourceIndex(), current[i].getAttributeIndex(), current[i].getName(), current[i].getValue());
						}
						
					}
					
				}
				else{
					
					for(int i = 0; i < current.length; i++){
						
						aggregate(result[i], current[i], getAggregationType(current[i]), numSeenSamples);
						
					}
					
					
					
				}
				
				numSeenSamples++;
				
			}
			
			
			if(result != null){
				
				
				FileOutputStream fos = null;
				PrintStream out = null;
				try {
					boolean exists = false;
					File fileFolder = new File("log/csv/cluster");
					
					exists = fileFolder.exists();
					if(!exists){
						fileFolder.mkdirs();
					}
					
					File file = new File(fileFolder,""+res.toString()+".csv");
					
					exists = file.exists();
					
					fos = new FileOutputStream(file, true);
					out = new PrintStream(fos);
					String line;
					String header;
					if(out != null){
						line="";
						header="";
						for(int i = 0; i<result.length; i++){
							
							if(result[i] != null){
								header+=""+result[i].getName();
								line+=""+result[i].getValue();
								
								if(i!=result.length-1 && result[i+1] != null){
									header+=",";
									line+=",";
									
								}
							}	
							
						}
						if(!exists){
							out.println(header);
						}
						out.println(line);
						out.flush();
						
						
					}
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
				finally{
					if(out != null){
						out.close();
					}
					
					if(fos != null){
						try {
							fos.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
				
			}
			
			
			
		}
		
		
	}
	*/
	private void aggregate(PublishAttribute result, PublishAttribute newSample, Aggregation aggrType, int numSeenSamples){
		
		if(result == null || newSample == null){
			return;
		}
		
		Object partialValue = result.getValue();
		Object newSampleValue = newSample.getValue();
		
		
		
		if(partialValue == null || newSampleValue == null){
			
			return;
		}
		
		//Now I convert all these numeric values to Double!
		
		Double partialDoubleValue = 0.0D;
		Double newSampleDoubleValue = 0.0D;
		
		if(partialValue instanceof Integer){
			partialDoubleValue = ((Integer) partialValue) * 1.0D;
		}
		else if(partialValue instanceof Long){
			partialDoubleValue = ((Long) partialValue) * 1.0D;
		}
		else if (partialValue instanceof Float){
			partialDoubleValue = ((Float) partialValue) * 1.0D;
		}
		else if(partialValue instanceof Double){
			partialDoubleValue = (Double) partialValue;
		}
		else{
			return;
		}
		
		if(newSampleValue instanceof Integer){
			newSampleDoubleValue = ((Integer) newSampleValue) * 1.0D;
		}
		else if(newSampleValue instanceof Long){
			newSampleDoubleValue = ((Long) newSampleValue) * 1.0D;
		}
		else if (newSampleValue instanceof Float){
			newSampleDoubleValue = ((Float) newSampleValue) * 1.0D;
		}
		else if(newSampleValue instanceof Double){
			newSampleDoubleValue = (Double) newSampleValue;
		}
		else{
			return;
		}
		
		if(aggrType == Aggregation.SUM){
			
			
			result.setValue(partialDoubleValue + newSampleDoubleValue);
			
			
			
		}
		else if (aggrType == Aggregation.MEAN){
			if(numSeenSamples > 0){
				if(DEBUG){
					log.debug("AGGREGATION");
					log.debug("Name Result: "+result.getName());
					log.debug("Name New Sample: "+newSample.getName());
					log.debug("Old Result: "+partialDoubleValue);
					log.debug("New Sample: "+newSampleDoubleValue);
					log.debug("Num Seen Samples: "+numSeenSamples);
					
				}
				result.setValue((partialDoubleValue * (numSeenSamples*1.0D) + newSampleDoubleValue) / ((numSeenSamples+1)*1.0D));
				
				
				if(DEBUG){
					log.debug("Result: "+result.getValue());
					log.debug(" ");
				}
			}
		}
		
	}
	/*
	private PublishAttribute[] produceCSVFor(PublishStatisticsEvent pse, String IP, ResourceType resType, int resIndex){
		
		PublishMeasurement pm = pse.getPublishMeasurement(resType, resIndex, IP);
	
		FileOutputStream fos = null;
		PrintStream out = null;
		try {
			boolean exists = false;
			File fileFolder = new File("log/csv/"+IP);
			
			exists = fileFolder.exists();
			if(!exists){
				fileFolder.mkdirs();
			}
			
			File file = new File(fileFolder,""+IP+"_"+resType.toString()+"_"+resIndex+".csv");
			
			exists = file.exists();
			
			fos = new FileOutputStream(file, true);
			out = new PrintStream(fos);
			PublishAttribute[] attr = toArray(pm.getValues(), resIndex);
			String line;
			String header;
			if(out != null && attr!=null){
				line="";
				header="";
				for(int i = 0; i<attr.length; i++){
					
					if(attr[i] != null){
						header+=""+attr[i].getName();
						line+=""+attr[i].getValue();
						
						if(i!=attr.length-1){
							header+=",";
							line+=",";
							
						}
					}	
					
				}
				if(!exists){
					out.println(header);
				}
				out.println(line);
				out.flush();
				
				return attr;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		finally{
			if(out != null){
				out.close();
			}
			
			if(fos != null){
				try {
					fos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return null;
		
	}
	*/
    private PublishAttribute[][] produceCSVFor(PublishStatisticsEvent pse, String IP, ResourceType resType, long timestamp){
		
    	int numResources = pse.getNumResources(resType, IP);
    	int numAttributes = 0;
    	if(numResources > 0){

    		PublishAttribute[][] allAttr = new PublishAttribute[numResources][];
    		PublishAttribute[] current;
    		PublishMeasurement pm;
    		for(int i = 0; i < numResources; i++){
    			pm = pse.getPublishMeasurement(resType, i, IP); 
    			current = toArray(pm.getValues(), i, resType);
    			allAttr[i] = current;
    			if(current != null){
    				numAttributes = current.length;
    			}
    		}


    		FileOutputStream fosNumeric = null;
    		PrintStream outNumeric = null;
    		FileOutputStream fosOthers = null;
    		PrintStream outOthers = null;
    		
    		FileOutputStream fosTopKeys = null;
    		PrintStream outTopKeys = null;
    		try {
    			boolean existsNumeric = false;
    			boolean existsOthers = false;
    			boolean existsTopKeys = false;
    			
    			boolean atLeastOneNumeric = false;
    			boolean atLeastOneOthers = false;
    			boolean atLeastOneTopKeys = false;
    			
    			File fileFolder = new File("log/csv/"+IP);

    			existsNumeric = fileFolder.exists();
    			if(!existsNumeric){
    				fileFolder.mkdirs();
    			}

    			File fileNumeric = new File(fileFolder,""+IP+"_"+resType.toString()+"_numeric.csv");
    			File fileOthers = new File(fileFolder,""+IP+"_"+resType.toString()+"_others.csv");
    			
    			File fileTopKeys = new File(fileFolder,""+IP+"_"+resType.toString()+"_topKeys.csv");
    			if(resType.equals(ResourceType.JMX)){
    				fileTopKeys = new File(fileFolder,""+IP+"_"+resType.toString()+"_topKeys.csv");
    			}	

    			existsNumeric = fileNumeric.exists();
    			existsOthers = fileOthers.exists();
    			
    			if(resType.equals(ResourceType.JMX)){
    				existsTopKeys = fileTopKeys.exists();
    			}

    			fosNumeric = new FileOutputStream(fileNumeric, true);
    			outNumeric = new PrintStream(fosNumeric);
    			fosOthers = new FileOutputStream(fileOthers, true);
    			outOthers = new PrintStream(fosOthers);
    			if(resType.equals(ResourceType.JMX)){
    				fosTopKeys = new FileOutputStream(fileTopKeys, true);
    				outTopKeys = new PrintStream(fosTopKeys);
    			}
    			
    			String lineNumeric;
    			String headerNumeric;
    			String lineOthers;
    			String headerOthers;
    			String lineTopKeys;
    			String headerTopKeys;
    			if(outNumeric != null && numAttributes != 0){
    				lineNumeric=""+timestamp+",";
    				headerNumeric="Timestamp,";
    				lineOthers=""+timestamp+",";
    				headerOthers="Timestamp,";
    				lineTopKeys=""+timestamp+",";
    				headerTopKeys="Timestamp,";
    				
    				for(int i =0; i < numResources; i++){

    					for(int j = 0; j<numAttributes; j++){

    						if(allAttr[i][j] != null){
    							if(allAttr[i][j].isNumeric() || allAttr[i][j].isBoolean()){
    								atLeastOneNumeric = true;
    								headerNumeric+=""+allAttr[i][j].getName()+"_"+i;
    								if(allAttr[i][j].isBoolean()){
    									if(allAttr[i][j].getValue() != null){
    										if(((Boolean)allAttr[i][j].getValue()) == true){
    											lineNumeric+=""+"1";
    										}
    										else{
    											lineNumeric+=""+"0";
    										}
    									}
    								}
    								else{
    									lineNumeric+=""+allAttr[i][j].getValue();
    								}
    								

    								if(i != numResources -1 || j != numAttributes -1){
    									headerNumeric+=",";
    									lineNumeric+=",";

    								}
    							}
    							else{
    								if(resType.equals(ResourceType.JMX) && isTopKey(allAttr[i][j])){
    									atLeastOneTopKeys = true;
    									headerTopKeys+=""+allAttr[i][j].getName()+"_"+i;
    									lineTopKeys+=""+allAttr[i][j].getValue();

    									if(i != numResources -1 || j != numAttributes -1){
    										headerTopKeys+=",";
    										lineTopKeys+=",";

    									}
    								}
    								else{
    									atLeastOneOthers = true;
    									headerOthers+=""+allAttr[i][j].getName()+"_"+i;
    									lineOthers+=""+allAttr[i][j].getValue();

    									if(i != numResources -1 || j != numAttributes -1){
    										headerOthers+=",";
    										lineOthers+=",";

    									}
    								}

    							}
    						}	

    					}

    				}
    				
    				if(atLeastOneNumeric){
    					if(!existsNumeric){
    						outNumeric.println(headerNumeric);
    					}
    					outNumeric.println(lineNumeric);
    					outNumeric.flush();
    				}
    				if(atLeastOneOthers){
    					if(!existsOthers){
    						outOthers.println(headerOthers);
    					}
    					outOthers.println(lineOthers);
    					outOthers.flush();
    				}
    				if(atLeastOneTopKeys){
    					if(!existsTopKeys){
    						outTopKeys.println(headerTopKeys);
    					}
    					outTopKeys.println(lineTopKeys);
    					outTopKeys.flush();
    				}
    				
    				

    				return allAttr;
    			}
    		} catch (FileNotFoundException e) {
    			e.printStackTrace();
    		}
    		finally{
    			if(outNumeric != null){
    				outNumeric.close();
    			}

    			if(fosNumeric != null){
    				try {
    					fosNumeric.close();
    				} catch (IOException e) {
    					e.printStackTrace();
    				}
    			}
    		}
    	}

    	return null;
		
	}
    
    
	
	private boolean checkJmxMembers(Measurement mis) {
	
		boolean result = false;
		
		if(mis != null && mis.getResourceType()!=null && mis.getResourceType().equals(ResourceType.JMX)){
			
			Long timestamp = this.jmxNodes.get(mis.getIp());
			
			if(timestamp == null){//New Member
				
				System.out.println("New member detected: "+mis.getIp());
				result = true;
				
			}
			
			this.jmxNodes.put(mis.getIp(), System.currentTimeMillis());
			
			
			ArrayList<MeasurementAttribute> list = mis.getMeasurementAttributes();
			
			for(MeasurementAttribute ma: list){
				
				if(ma.getShort_name().equalsIgnoreCase("numNodes")){
					this.numJmxNodes = Long.parseLong(ma.getValue());
					
				}
				
			}
			
			this.numCheckJmxNodes++;
			
			if(numCheckJmxNodes > 2*this.numJmxNodes){
				
				numCheckJmxNodes = 0;
				
				Set<Entry<String, Long>> set = this.jmxNodes.entrySet();
				List<KnownMember> all = new LinkedList<KnownMember>();
				
				long now = System.currentTimeMillis();
				
				for(Entry<String,Long> entry: set){
					
					all.add(new KnownMember(entry.getKey(), entry.getValue() - now)); //entry.getValue() - now is correct. We want that member with small delta (in this case a big negative number) go out.
					
				}
				
				Collections.sort(all);
				
				int toRemove = (all.size()) - (int)this.numJmxNodes;
				
				if(toRemove > 0){
					
					result = true;
					KnownMember removed;
					for(int i=0; i< toRemove; i++){
						
						removed = all.remove(0);
						System.out.println("Removed: "+removed);
					}
					
					
					this.jmxNodes.clear();
					
					for(KnownMember m: all){
						
						this.jmxNodes.put(m.address, now);
						
					}
					
				}
				
			}
		}
		
		
		return result;
	}
	
	private static PublishAttribute[] toArray(Map<String, PublishAttribute> attr, int resIndex, ResourceType resType){
		
		
		
		if(attr!=null){
			
			int size = attr.size();
			int offset;
			if(size > 0){
				PublishAttribute[] result = new PublishAttribute[size];
				Collection<PublishAttribute> collection = attr.values();
					
				for(PublishAttribute pa: collection){
					
					
					offset = ClusterWidePublishAttributeIndexGenerator.getInstance().getId(pa.getName(), resType);
					result[offset] = pa;
					
					
					
				}
				
				
				return result;
			}
			
		}
		
		
		return null;
	}
	
	
	private Aggregation getAggregationType(PublishAttribute attr){
		
		Aggregation a = null;
		if(attr!=null){
			if(this.aggregationTypes != null){
				a = aggregationTypes.get(attr.getName().toLowerCase());
				
			}
			
			if(a==null){
				if((attr.getValue() instanceof Integer) ||
						(attr.getValue() instanceof Long)||
						(attr.getValue() instanceof Double)||
						(attr.getValue() instanceof Float)){
					a = Aggregation.MEAN;
					
				}
				else{
					a=Aggregation.NO;
					
				}
			}
		}
		else{
			a = Aggregation.NO;
		}
		
		return a;
		
		/*
		if(attr != null){
			
			//CPU
			if(attr.getName().equalsIgnoreCase("CpuPerc.sys")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("CpuPerc.user")){
				return Aggregation.MEAN;
			}

			//NETWORK
			else if(attr.getName().equalsIgnoreCase("receivedBytes")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("transmittedBytes")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("receivedBytesPerSecond")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("transmittedBytesPerSecond")){
				return Aggregation.SUM;
			}


			//DISK
			else if(attr.getName().equalsIgnoreCase("fileSystemUsage.usePercent")){
				return Aggregation.MEAN;
			}


			//MEMORY
			else if(attr.getName().equalsIgnoreCase("MemoryInfo.free")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("MemoryInfo.used")){
				return Aggregation.SUM;
			}



			//JMX
			else if(attr.getName().equalsIgnoreCase("ConcurrencyLevel")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("NumberOfLocksAvailable")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("NumberOfLocksHeld")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("SuccessRatioFloatingPoint")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AverageReplicationTime")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("ReplicationFailures")){
				return Aggregation.SUM;
			}
			//else if(attr.getName().equalsIgnoreCase("SuccessRatio")){
			//	return Aggregation.MEAN;
			//}
			else if(attr.getName().equalsIgnoreCase("ReplicationCount")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("AverageWriteTime")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("TimeSinceReset")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AverageReadTime")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("HitRatio")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("NumberOfEntries")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("Evictions")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("RemoveMisses")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("ReadWriteRatio")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("ElapsedTime")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("Hits")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("RemoveHits")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("Stores")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("Misses")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("CoolDownTime")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("MaxNumberOfKeysToRequest")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("UsagePercentage")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("CorePoolSize")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("MaximumPoolSize")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("KeepAliveTime")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgRollbackAsync")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgLocalLockHoldTime")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgRemoteTxCompleteNotifyTime")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("PercentageWriteTransactions")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgLocalCommitTime")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("NumberOfRemotePuts")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("AvgWriteTxLocalExecution")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgWriteTxDuration")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgRemotePrepareTime")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgLocalGetTime")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgLockWaitingTime")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgRollbackTime")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgRollbackRtt")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgNumOfLockSuccessLocalTx")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("NumAbortedTxDueTimeout")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("LocalContentionProbability")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgCommitAsync")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("NumAbortedTxDueDeadlock")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("RemoteGetExecutionTime")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgLocalRollbackTime")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgPutsPerWrTransaction")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgTCBTime")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgNumOfLockRemoteTx")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgCompleteNotificationAsync")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgClusteredGetCommandSize")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgResponseTime")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgRemoteRollbackTime")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("ReplicationDegree")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("NumberOfCommits")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("NumNodes")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgNTCBTime")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgNumPutsBySuccessfulLocalTx")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgPrepareRtt")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgRemoteLockHoldTime")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("WriteSkewProbability")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgLocalPrepareTime")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgRemoteGetsPerROTransaction")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("PercentageSuccessWriteTransactions")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgNumNodesCompleteNotification")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgNumNodesPrepare")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("ApplicationContentionFactor")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("NumberOfGets")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("AvgLockHoldTime")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgReadOnlyTxDuration")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgRemoteGetsPerWrTransaction")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgCommitCommandSize")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgCommitRtt")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgRemotePutsPerWrTransaction")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("RemoteContentionProbability")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgRemoteGetRtt")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgNumNodesRemoteGet")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("NumberOfRemoteGets")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("AvgPrepareCommandSize")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("NumberOfPuts")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("AvgNumOfLockLocalTx")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgTxArrivalRate")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("LocalExecutionTimeWithoutLock")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgRemoteCommitTime")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgAbortedWriteTxDuration")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgCommitTime")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("RemotePutExecutionTime")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AbortRate")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgGetsPerWrTransaction")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgNumNodesRollback")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgNumNodesCommit")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("NumberOfLocalCommits")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("LockContentionProbability")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("Throughput")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("AvgPrepareAsync")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgGetsPerROTransaction")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("LocalActiveTransactions")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("LocalRollbacks")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("Prepares")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("Rollbacks")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("LocalCommits")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("Commits")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("AvgFailedTxCommit")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("AvgSuccessfulTxCommit")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("SuccessfulCommits")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("FailedCommits")){
				return Aggregation.MEAN;
			}
			else if(attr.getName().equalsIgnoreCase("LocalPrepares")){
				return Aggregation.SUM;
			}
			else if(attr.getName().equalsIgnoreCase("AverageValidationDuration:")){
				return Aggregation.MEAN;
			}
			
			

		}
		return Aggregation.NO;
		
		*/
	}
	
	private boolean isTopKey(PublishAttribute attr){
		
		if(attr != null){
			
			String name = attr.getName();
			
			if(name.equalsIgnoreCase("TopLockFailedKeys")){
				return true;
			}
			else if(name.equalsIgnoreCase("RemoteTopGets")){
				return true;
			}
			else if(name.equalsIgnoreCase("TopWriteSkewFailedKeys")){
				return true;
			}
			else if(name.equalsIgnoreCase("LocalTopPuts")){
				return true;
			}
			else if(name.equalsIgnoreCase("TopContendedKeys")){
				return true;
			}
			else if(name.equalsIgnoreCase("RemoteTopPuts")){
				return true;
			}
			else if(name.equalsIgnoreCase("LocalTopGets")){
				return true;
			}
			else if(name.equalsIgnoreCase("TopLockedKeys")){
				return true;
			}
			
		}
		
		return false;
	}

	private void loadParametersFromRegistry(){
    	String propsFile = "config/log_service.config";
    	Properties props = new Properties();
		try {
			props.load(new FileInputStream(propsFile));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		cacheName = props.getProperty("Cache_name");
		//infinispanConfigFile = props.getProperty("InfinispanConfigFile");
		timeout = Long.parseLong(props.getProperty("AnalyzerThreadTimeout"));
		enableInfinispan = Boolean.parseBoolean(props.getProperty("enableInfinispan"));
		enableListeners = Boolean.parseBoolean(props.getProperty("enableListeners"));
		
		String rmiPortNumber = props.getProperty("RMIPort_number");
		if(rmiPortNumber != null){
			RMI_REGISTRY_PORT = Integer.parseInt(rmiPortNumber);
		}
    }
	
	private enum Aggregation{
		
		NO, MEAN, SUM
		
		
	}
	
	private void loadAggregationTypesFromRegistry(){
    	String propsFile = "config/stats_aggregation.config";
    	Properties props = new Properties();
		try {
			props.load(new FileInputStream(propsFile));
			
			this.aggregationTypes = new HashMap<String, Aggregation>();
			
			Set<String> keys = props.stringPropertyNames();
			String value;
			for(String k: keys){
				
				value = props.getProperty(k);
				
				if("MEAN".equalsIgnoreCase(value)){
					this.aggregationTypes.put(k.toLowerCase(), Aggregation.MEAN);
				}
				else if("SUM".equalsIgnoreCase(value)){
					this.aggregationTypes.put(k.toLowerCase(), Aggregation.SUM);
				}
				else if("NONE".equalsIgnoreCase(value)){
					this.aggregationTypes.put(k.toLowerCase(), Aggregation.NO);
				}
				
			}
			
			
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
    }
		
		
	private class KnownMember implements Comparable<KnownMember>{
		String address;
		
		long delta;
		
		
		public KnownMember(String address, long delta){
			
			
			this.address = address;
			this.delta = delta;
		}

		@Override
		public int compareTo(KnownMember arg0) {
			
			if(arg0 == null){
				
				return -1;
			}
			
			if(delta < arg0.delta){
				return -1;
			}
			else if(delta > arg0.delta){
				return 1;
			}
			else{
				return 0;
			}
		}

		@Override
		public String toString() {
			return "KnownMember [address=" + address + ", delta=" + delta + "]";
		}
		
		
		
	}	
		
	
	
	
}
