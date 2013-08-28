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

package eu.cloudtm.wpm.sw_probe;

import eu.cloudtm.resources.MonitorableResources;
import eu.reservoir.monitoring.core.*;
import org.apache.log4j.Logger;

import javax.management.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import static eu.cloudtm.wpm.sw_probe.SoftwareProbeUtil.cleanCollection;
import static eu.cloudtm.wpm.sw_probe.SoftwareProbeUtil.cleanValue;

/**
* @author Roberto Palmieri
* @author Sebastiano Peluso
*/
public class InfinispanResourceProbe extends AbstractProbe implements Probe {

	private final static Logger log = Logger.getLogger(InfinispanResourceProbe.class);
	private final static boolean INFO = log.isInfoEnabled();
    private static final String TRANSACTION_CLASS_SUFFIX = "ForTxClass";
	private static final String[] TRANSACTION_CLASS_SIGNATURE = new String[] {"java.lang.String"};

	static InfinispanInfo monitored_infinispan;
	static String addr;
	static int port_num;
	static String cache_name;
	static String jmxDomain_primary;
	static String jmxDomain_secondary;
	static String replicationType;
	static String cacheManager;
	static boolean JBossEnabled;
	static boolean resetStatis;
    private String[] transactionClasses;
    private final List<String> transactionClassesMethodName;

	//probe_timeout in millisecond
	public InfinispanResourceProbe(String name,int probe_timeout){
		setName(name);
        transactionClassesMethodName = new ArrayList<String>();
		//Logical group of VM
		//ID gr_id = new ID(group_id);
		//setGroupID(gr_id);
		//Specified in measurements per hour
		int milliseconds_each_hour = 3600000;
		Rational probe_rate = new Rational(milliseconds_each_hour,1000);
		try{
			probe_rate = new Rational(milliseconds_each_hour, probe_timeout);
		}catch(Exception e){
			e.printStackTrace();
		}
        setDataRate(probe_rate);
        //retrive JMX and infinispan parameters
        loadParametersFromRegistry();
        monitored_infinispan = new InfinispanInfo(addr, port_num, cache_name, jmxDomain_primary,replicationType,cacheManager,JBossEnabled,resetStatis,jmxDomain_secondary);
        setProbeAttributesRuntime();
        //setProbeAttributes(cache_name);
	}

	private void setProbeAttributesRuntime(){
		try {
			int attributeKey = 0;
			ObjectName obj_mbeans;
			obj_mbeans = new ObjectName(jmxDomain_primary+":type=Cache,*");
			Set<ObjectName> mbeans = monitored_infinispan.mbsc.queryNames(obj_mbeans, null);
			if(mbeans == null || mbeans.size() == 0){
				obj_mbeans = new ObjectName(jmxDomain_secondary+":type=Cache,*");
				mbeans = monitored_infinispan.mbsc.queryNames(obj_mbeans, null);
			}
	        for(ObjectName on : mbeans) {
	        	if(on.getCanonicalName().contains("component=Cache"))
	        		continue;
                final MBeanInfo mBeanInfo = monitored_infinispan.mbsc.getMBeanInfo(on);
	        	MBeanAttributeInfo[] atts = mBeanInfo.getAttributes();
	        	for(MBeanAttributeInfo att : atts){
	        		//control added to be safe in case the name is null
	        		if(att.getName() == null){
	        			continue;
	        		}
	        		String nameAttribute = capitalize(att.getName());
	        		if(nameAttribute.equalsIgnoreCase("statisticsEnabled"))
	        			continue;
					if(att.getType().equals("double")){
						addProbeAttribute(new DefaultProbeAttribute(attributeKey++, nameAttribute, ProbeAttributeType.DOUBLE, ""));
					}else if(att.getType().equals("long")){
						addProbeAttribute(new DefaultProbeAttribute(attributeKey++, nameAttribute, ProbeAttributeType.LONG, ""));
					}else if(att.getType().equals("int")){
						addProbeAttribute(new DefaultProbeAttribute(attributeKey++, nameAttribute, ProbeAttributeType.INTEGER, ""));
					}else if(att.getType().equals("java.lang.String")){
						addProbeAttribute(new DefaultProbeAttribute(attributeKey++, nameAttribute, ProbeAttributeType.STRING, ""));
					}else if(att.getType().equals("java.util.Map")){
						addProbeAttribute(new DefaultProbeAttribute(attributeKey++, nameAttribute, ProbeAttributeType.STRING, ""));
					}else if(att.getType().equals("boolean")){
						addProbeAttribute(new DefaultProbeAttribute(attributeKey++, nameAttribute, ProbeAttributeType.BOOLEAN, ""));
					}else if(att.getType().equals("short")){
						addProbeAttribute(new DefaultProbeAttribute(attributeKey++, nameAttribute, ProbeAttributeType.SHORT, ""));
					}else{
						addProbeAttribute(new DefaultProbeAttribute(attributeKey++, nameAttribute, ProbeAttributeType.STRING, ""));
						if(INFO)
							log.info(att.getType());
						//throw new RuntimeException("Format not supported!");
					}
					//System.out.println("Added "+att.getName()+":"+(attributeKey-1));
					//System.out.println(att.getType());
	        	}

                //for the transaction classes
                if (transactionClasses != null && transactionClasses.length != 0 &&
                        "ExtendedStatistics".equals(on.getKeyProperty("component"))) {
                    if (INFO) {
                        log.info("Registering transaction class statistics for " + on + "...");
                    }
                    MBeanOperationInfo[] operationInfos = mBeanInfo.getOperations();
                    for (MBeanOperationInfo operationInfo : operationInfos) {
                        String attributeName = extractTransactionClassAttributeName(operationInfo);
                        if (attributeName == null) {
                            continue; //not a transaction class method
                        }
                        if (log.isInfoEnabled()) {
                            log.info("Method " + operationInfo.getName() + " is a transaction class statistic!");
                        }
                        transactionClassesMethodName.add(operationInfo.getName());
                        String attributeType = operationInfo.getReturnType();
                        ProbeAttributeType probeAttributeType;
                        if (attributeType.equals("double")) {
                            probeAttributeType = ProbeAttributeType.DOUBLE;
                        } else if (attributeType.equals("long")) {
                            probeAttributeType = ProbeAttributeType.LONG;
                        } else if (attributeType.equals("int")) {
                            probeAttributeType = ProbeAttributeType.INTEGER;
                        } else if (attributeType.equals("boolean")) {
                            probeAttributeType = ProbeAttributeType.BOOLEAN;
                        } else if (attributeType.equals("short")) {
                            probeAttributeType = ProbeAttributeType.SHORT;
                        } else {
                            probeAttributeType = ProbeAttributeType.SHORT;
                        }
                        for (String txClass : transactionClasses) {
                            addProbeAttribute(new DefaultProbeAttribute(attributeKey++,
                                    createProbeAttributeName(txClass, attributeName), probeAttributeType, ""));
                        }
                    }
                } else if (INFO) {
                    log.info("Do not register transaction statistics for " + on);
                }

	        	//invoking_method.invoke(inf_value,(Double)mbsc.getAttribute(statisticsComponent,f.getName()));

	        	//System.out.println("---\n");
	        }
		} catch (MalformedObjectNameException e) {
			e.printStackTrace();
		} catch (NullPointerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InstanceNotFoundException e) {
			e.printStackTrace();
		} catch (IntrospectionException e) {
			e.printStackTrace();
		} catch (ReflectionException e) {
			e.printStackTrace();
		} catch (Exception e){
			e.printStackTrace();
		}
	}

    /**
     * @return the attribute name or null if the method is not a transaction class method.
     */
    private String extractTransactionClassAttributeName(MBeanOperationInfo method) {
        String methodName = method.getName();
        if (log.isDebugEnabled()) {
            log.debug("Analyzing method for transaction statistics: " + methodName);
        }
        //first check the signature. it should a single String parameter.
        MBeanParameterInfo[] signature = method.getSignature();
        if (signature.length != 1) {
            //wrong signature
            if (log.isDebugEnabled()) {
                log.debug("[" + methodName + "] wrong signature");
            }
            return null;
        }
        String type = signature[0].getType();
        if (!"String".equals(type) && !"java.lang.String".equals(type)) {
            if (log.isDebugEnabled()) {
                log.debug("[" + methodName + "] wrong signature type: " + type);
            }
            //wrong signature
            return null;
        }

        //correct signature... check the method name
        if (!methodName.endsWith(TRANSACTION_CLASS_SUFFIX)) {
            if (log.isDebugEnabled()) {
                log.debug("[" + methodName + "] wrong method name");
            }
            return null;
        }
        String attributeName;
        int endIndex = methodName.indexOf(TRANSACTION_CLASS_SUFFIX);
        if (methodName.startsWith("get") || methodName.startsWith("set")) {
            attributeName = methodName.substring(3, endIndex);
        } else if (methodName.startsWith("is")) {
            attributeName = methodName.substring(2, endIndex);
        } else {
            attributeName = methodName.substring(0, endIndex);
        }
        if (log.isDebugEnabled()) {
            log.debug("[" + methodName + "] Statistic found! Capitalizing " + attributeName);
        }
        return capitalize(attributeName);
    }

    private String capitalize(String string) {
        if (Character.isUpperCase(string.charAt(0))) {
            return string;
        }
        return string.substring(0,1).toUpperCase() + string.substring(1);
    }

	public ProbeMeasurement collect() {
		if(INFO)
			log.info("Start collecting Infinispan Probe at: "+System.currentTimeMillis());
		// list of proble values
		ArrayList<ProbeValue> list = new ArrayList<ProbeValue>();
		int attributeKey = 0;
		try {
			ObjectName obj_mbeans;
			obj_mbeans = new ObjectName(jmxDomain_primary+":type=Cache,*");
			Set<ObjectName> mbeans = monitored_infinispan.mbsc.queryNames(obj_mbeans, null);
			if(mbeans == null || mbeans.size() == 0){
				obj_mbeans = new ObjectName(jmxDomain_secondary+":type=Cache,*");
				mbeans = monitored_infinispan.mbsc.queryNames(obj_mbeans, null);
			}
	        for(ObjectName on : mbeans) {
	        	if(on.getCanonicalName().contains("component=Cache"))
	        		continue;
	        	MBeanAttributeInfo[] atts = monitored_infinispan.mbsc.getMBeanInfo(on).getAttributes();
	        	for(MBeanAttributeInfo att : atts){
	        		//control added to be safe in case the name is null
	        		if(att.getName() == null){
	        			continue;
	        		}
	        		if(att.getName().equalsIgnoreCase("statisticsEnabled"))
	        			continue;
	        		try{
	        			Object value = monitored_infinispan.mbsc.getAttribute(on,att.getName());
	        			if(value instanceof Map){
	    			    	list.add(new DefaultProbeValue(attributeKey++, cleanCollection(String.valueOf(value))));
	    				}else if(value instanceof String){
	    					list.add(new DefaultProbeValue(attributeKey++, cleanValue(String.valueOf(value))));
	    				}else{
	    					list.add(new DefaultProbeValue(attributeKey++, value));
	    				}
		        		//System.out.println("Value "+att.getName()+":"+(attributeKey-1)+":"+value);
	        		}catch(Exception e){
	        			e.printStackTrace();
	        			list.add(new DefaultProbeValue(attributeKey++, "xx"));
	        		}
	        	}

                //for the transaction classes
                if (transactionClasses != null && transactionClasses.length != 0 &&
                        "ExtendedStatistics".equals(on.getKeyProperty("component"))) {
                    for (String method : transactionClassesMethodName) {
                        for (String txClass : transactionClasses) {
                            try {
                                Object value = monitored_infinispan.mbsc.invoke(on, method, new Object[] {txClass},
                                        TRANSACTION_CLASS_SIGNATURE);
                                if (value instanceof Map) {
                                    list.add(new DefaultProbeValue(attributeKey++, cleanCollection(String.valueOf(value))));
                                } else if (value instanceof String) {
                                    list.add(new DefaultProbeValue(attributeKey++, cleanValue(String.valueOf(value))));
                                } else {
                                    list.add(new DefaultProbeValue(attributeKey++, value));
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                                list.add(new DefaultProbeValue(attributeKey++, "xx"));
                            }
                        }
                    }
                }

	        	if(resetStatis){
	        		//reset statistics specified MBean
		        	try {
						monitored_infinispan.mbsc.invoke(on, "resetStatistics", new Object[]{}, new String[]{});
					} catch (MBeanException e) {
                        //ignored
					}
	        	}
	        }


		} catch (MalformedObjectNameException e) {
			e.printStackTrace();
		} catch (NullPointerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InstanceNotFoundException e) {
			e.printStackTrace();
		} catch (IntrospectionException e) {
			e.printStackTrace();
		} catch (ReflectionException e) {
			e.printStackTrace();
		} catch (TypeException e) {
			e.printStackTrace();
		} catch(Exception e){
			e.printStackTrace();
		}
		return new ProducerMeasurement(this, list, MonitorableResources.JMX.toString());
    }

	private void loadParametersFromRegistry(){
    	String propsFile = "config/infinispan_probe.config";
    	Properties props = new Properties();
		try {
			props.load(new FileInputStream(propsFile));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		port_num = Integer.parseInt(props.getProperty("Infinispan_JMX_port_number"));
		cache_name = props.getProperty("Cache_name");
		jmxDomain_primary = props.getProperty("JMXDomain_primary");
		jmxDomain_secondary = props.getProperty("JMXDomain_secondary");
		replicationType = props.getProperty("Replication_type");
		cacheManager = props.getProperty("Cache_Manager");
		JBossEnabled = Boolean.parseBoolean(props.getProperty("UseJBoss"));
        String txClassList = props.getProperty("TransactionClasses");
        if (txClassList != null && !txClassList.isEmpty()) {
            Set<String> txClassesSet = new HashSet<String>(Arrays.asList(txClassList.split(",")));
            txClassesSet.remove("DEFAULT_ISPN_CLASS"); //already collected
            transactionClasses = txClassesSet.toArray(new String[txClassesSet.size()]);
        }
		addr = props.getProperty("Infinispan_IP_Address");
		if(addr == null || addr.equals("")){
			try {
				InetAddress thisIp = InetAddress.getLocalHost();
				addr = thisIp.getHostAddress();
				if(INFO)
					log.info("Infinispan probe attached to "+addr);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}
		resetStatis = Boolean.parseBoolean(props.getProperty("ResetStats"));

    }

    private String createProbeAttributeName(String txClass, String attributeName) {
        return txClass.replaceAll("\\.+","_").replaceAll(",+","_") + "." + attributeName;
    }
}
