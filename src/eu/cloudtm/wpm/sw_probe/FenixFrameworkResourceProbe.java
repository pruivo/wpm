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
import javax.management.remote.JMXServiceURL;
import java.io.FileInputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import static eu.cloudtm.wpm.sw_probe.SoftwareProbeUtil.cleanCollection;
import static eu.cloudtm.wpm.sw_probe.SoftwareProbeUtil.cleanValue;
import static java.lang.String.valueOf;
import static java.util.concurrent.TimeUnit.HOURS;

/**
 * @author Pedro Ruivo
 * @since 1.0
 */
public class FenixFrameworkResourceProbe extends AbstractProbe implements Probe {

    private final static Logger log = Logger.getLogger(FenixFrameworkResourceProbe.class);
    private final static boolean INFO = log.isInfoEnabled();
    private static final String PROPERTY_FILE = "config/fenix_probe.properties";
    private static final int RETRY_TIME_MSEC = 5000;
    private final boolean enabled;
    private final List<MBeanInfo> mBeanInfoList;
    private String applicationName;
    private String jmxDomain;
    private boolean reset;
    private MBeanServerConnection mBeanServerConnection;
    private volatile boolean errorResetingStats = false;

    //probe_timeout in millisecond
    public FenixFrameworkResourceProbe(String name, int probeTimeout) {
        setName(name);
        this.mBeanInfoList = new ArrayList<MBeanInfo>();
        //Specified in measurements per hour
        int millisecondsEachHour = (int) HOURS.toMillis(1);
        Rational probeRate = new Rational(millisecondsEachHour, 1000);
        try {
            probeRate = new Rational(millisecondsEachHour, probeTimeout);
        } catch (Exception e) {
            e.printStackTrace();
        }
        setDataRate(probeRate);
        boolean tmpEnabled = loadPropertiesAndConnect();
        if (tmpEnabled) {
            try {
                awaitMBeans();
            } catch (Exception e) {
                log.error("Error awaiting for Fenix Framework MBeans to be registered", e);
                tmpEnabled = false;
            }
        }
        this.enabled = tmpEnabled;
    }

    @Override
    public ProbeMeasurement collect() {
        if (!enabled) {
            return new ProducerMeasurement(this, Collections.<ProbeValue>emptyList(), MonitorableResources.FENIX.toString());
        }
        if (INFO) {
            log.info("Start collecting Fenix Framework Probe at: " + System.currentTimeMillis());
        }
        // list of probe values
        List<ProbeValue> list = new ArrayList<ProbeValue>();
        for (MBeanInfo mBeanInfo : mBeanInfoList) {
            collectForMBean(mBeanInfo, list);
            resetMBean(mBeanInfo.objectName);
        }

        return new ProducerMeasurement(this, list, MonitorableResources.FENIX.toString());
    }

    private void collectForMBean(MBeanInfo mBeanInfo, List<ProbeValue> valueList) {
        Set<String> attributeListToCollect = mBeanInfo.copyToSet();
        try {
            AttributeList attributesValues = mBeanServerConnection.getAttributes(mBeanInfo.objectName,
                    mBeanInfo.attributes());
            for (Attribute att : attributesValues.asList()) {
                if (safeAdd(valueList, mBeanInfo, att.getName(), att.getValue())) {
                    attributeListToCollect.remove(att.getName());
                }
            }
        } catch (Exception e) {
            log.error("Error collecting statistic for " + mBeanInfo.objectName, e);
        }
        for (String missingAttribute : attributeListToCollect) {
            safeAddDefault(valueList, mBeanInfo, missingAttribute);
        }
    }

    private boolean safeAdd(List<ProbeValue> valueList, MBeanInfo mBeanInfo, String attribute, Object value) {
        try {
            if (value instanceof Map || value instanceof Collection) {
                valueList.add(mBeanInfo.toProbeValue(attribute, cleanCollection(valueOf(value))));
            } else if (value instanceof String) {
                valueList.add(mBeanInfo.toProbeValue(attribute, cleanValue(valueOf(value))));
            } else {
                valueList.add(mBeanInfo.toProbeValue(attribute, value));
            }
            return true;
        } catch (TypeException e) {
            log.error("Type Exception for " + mBeanInfo.objectName + " and attribute " + attribute, e);
        }
        return false;
    }

    private void safeAddDefault(List<ProbeValue> valueList, MBeanInfo mBeanInfo, String attribute) {
        try {
            valueList.add(mBeanInfo.toDefaultProbeValue(attribute));
        } catch (TypeException e) {
            //this should never happen
            log.fatal("Type Exception in default for " + mBeanInfo.objectName + " and attribute " + attribute, e);
        }
    }

    private void resetMBean(final ObjectName objectName) {
        if (reset) {
            //reset statistics for MBean
            try {
                mBeanServerConnection.invoke(objectName, "reset", new Object[]{}, new String[]{});
            } catch (Exception e) {
                if (!errorResetingStats) {
                    log.error("Error resetting statistic for " + objectName, e);
                    errorResetingStats = true; //log the error only once
                } else if (log.isDebugEnabled()) {
                    //in debug, notify always!
                    log.debug("Error resetting statistic for " + objectName, e);
                }

            }
        } else {
            if (INFO) {
                log.info("Skip reset of " + objectName);
            }
        }
    }

    private boolean loadPropertiesAndConnect() {
        Properties props = new Properties();
        try {
            props.load(new FileInputStream(PROPERTY_FILE));
        } catch (Exception e) {
            log.error("Error loading properties from " + PROPERTY_FILE, e);
            return false;
        }

        applicationName = props.getProperty("fenix.appName");
        jmxDomain = props.getProperty("jmx.domain");
        int port = Integer.parseInt(props.getProperty("jmx.port"));
        String address = props.getProperty("jmx.ip");
        boolean jbossJmx = Boolean.parseBoolean(props.getProperty("jmx.jboss"));
        reset = Boolean.parseBoolean(props.getProperty("fenix.resetStats"));

        if (address == null || address.equals("")) {
            try {
                InetAddress thisIp = InetAddress.getLocalHost();
                address = thisIp.getHostAddress();
            } catch (UnknownHostException e) {
                log.error("Unable to find JMX address", e);
                return false;
            }
        }
        if (INFO) {
            log.info("Fenix Framework probe attached to " + address);
        }
        try {
            JMXServiceURL url = jbossJmx ? JmxConnectionManager.jbossRemotingUrl(address, port) :
                    JmxConnectionManager.jmxRmiUrl(address, port);
            mBeanServerConnection = JmxConnectionManager.getInstance().getConnection(url);
        } catch (Exception e) {
            log.error("Unable to connect to remote JMX server", e);
            return false;
        }
        if (mBeanServerConnection == null) {
            log.error("Unable to connect to remote JMX server by unknown reasons");
            return false;
        }
        return true;
    }

    private void awaitMBeans() throws Exception {
        ObjectName query = new ObjectName(jmxDomain + ":application=" + ObjectName.quote(applicationName) +
                ",*");
        boolean isConnected = false;
        while (!isConnected) {
            Collection<ObjectName> mbeans = null;
            try {
                mbeans = mBeanServerConnection.queryNames(query, null);
            } catch (Exception e) {
                //no-op
            }
            isConnected = mbeans != null && !mbeans.isEmpty();

            if (isConnected) {
                if (INFO) {
                    log.info("Fenix Framework MBeans found!");
                }
                registerProbes(mbeans);
            } else {
                if (INFO) {
                    log.info("Unable to find Fenix Framework MBeans. Retrying in " + RETRY_TIME_MSEC + "msec...");
                }
                try {
                    Thread.sleep(RETRY_TIME_MSEC);
                } catch (InterruptedException e1) {
                    log.error("Interrupted while waiting for Fenix Framework MBeans...");
                    //interrupted
                    //restore thread interrupt signal
                    Thread.currentThread().interrupt();
                    //and return
                    return;
                }
            }
        }
    }

    private void registerProbes(Collection<ObjectName> mBeans) throws Exception {
        if (INFO) {
            log.info("Registering Fenix Framework MBeans...");
        }
        int attributeKey = 0;
        int offset = 0;
        for (ObjectName objectName : mBeans) {
            if (INFO) {
                log.info("Registering " + objectName);
            }
            String remoteApplication = objectName.getKeyProperty("remoteApplication");
            final boolean isThreadPool = remoteApplication == null;
            final boolean isDap = objectName.getKeyProperty("component").equals("DapRemoteManager");
            MBeanInfo mBeanInfo = new MBeanInfo(objectName, offset);
            MBeanAttributeInfo[] beanAttributeInfos = mBeanServerConnection.getMBeanInfo(objectName).getAttributes();
            for (MBeanAttributeInfo info : beanAttributeInfos) {
                //control added to be safe in case the name is null
                if (info.getName() == null) {
                    log.error("Attribute for " + objectName + " is null. " + info);
                    continue;
                }

                String displayName;
                if (isDap) {
                    displayName = String.format("DAP.%s", info.getName());
                } else if (isThreadPool) {
                    displayName = String.format("%s.%s", applicationName, info.getName());
                } else {
                    displayName = String.format("%s.messaging(%s).%s", applicationName, remoteApplication, info.getName());
                }


                if (INFO) {
                    log.info("Registering " + info.getName() + ". Display name=" + displayName);
                }

                Object defaultValue = 0;
                if (info.getType().equals("double")) {
                    addProbeAttribute(new DefaultProbeAttribute(attributeKey++, displayName, ProbeAttributeType.DOUBLE, ""));
                } else if (info.getType().equals("long")) {
                    addProbeAttribute(new DefaultProbeAttribute(attributeKey++, displayName, ProbeAttributeType.LONG, ""));
                } else if (info.getType().equals("int")) {
                    addProbeAttribute(new DefaultProbeAttribute(attributeKey++, displayName, ProbeAttributeType.INTEGER, ""));
                } else if (info.getType().equals("boolean")) {
                    addProbeAttribute(new DefaultProbeAttribute(attributeKey++, displayName, ProbeAttributeType.BOOLEAN, ""));
                } else if (info.getType().equals("short")) {
                    addProbeAttribute(new DefaultProbeAttribute(attributeKey++, displayName, ProbeAttributeType.SHORT, ""));
                } else {
                    addProbeAttribute(new DefaultProbeAttribute(attributeKey++, displayName, ProbeAttributeType.STRING, ""));
                    defaultValue = "N/A";
                }
                mBeanInfo.addAttribute(info.getName(), defaultValue);
            }
            offset += mBeanInfo.size();
            mBeanInfoList.add(mBeanInfo);
        }
    }

    private class MBeanInfo {
        private final ObjectName objectName;
        private final int offset;
        private final List<String> attributeList;
        private final List<Object> defaultValueList;

        private MBeanInfo(ObjectName objectName, int offset) {
            this.objectName = objectName;
            this.offset = offset;
            this.attributeList = new ArrayList<String>();
            defaultValueList = new ArrayList<Object>();
        }

        public final void addAttribute(String jmxName, Object defaultValue) {
            attributeList.add(jmxName);
            defaultValueList.add(defaultValue);
        }

        public final int size() {
            return attributeList.size();
        }

        public final String[] attributes() {
            String[] result = new String[attributeList.size()];
            //it is just an array copy :)
            return attributeList.toArray(result);
        }

        public final Set<String> copyToSet() {
            return new HashSet<String>(attributeList);
        }

        public final ProbeValue toProbeValue(String jmxName, Object value) throws TypeException {
            int index = attributeList.indexOf(jmxName);
            if (index == -1) {
                return null;
            }
            return new DefaultProbeValue(index + offset, value);
        }

        public final ProbeValue toDefaultProbeValue(String jmxName) throws TypeException {
            int index = attributeList.indexOf(jmxName);
            if (index == -1) {
                return null;
            }
            return new DefaultProbeValue(index + offset, defaultValueList.get(index));
        }
    }
}
