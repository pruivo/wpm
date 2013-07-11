package eu.cloudtm.wpm.sw_probe;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Manages all the JMX connection to other MBean Servers
 *
 * @author Pedro Ruivo
 * @since 1.0
 */
public class JmxConnectionManager {

    private static JmxConnectionManager connectionManager;
    private final ConcurrentMap<JMXServiceURL, ConnectionWrapper> connectionMap;

    private JmxConnectionManager() {
        connectionMap = new ConcurrentHashMap<JMXServiceURL, ConnectionWrapper>();
    }

    public static JmxConnectionManager getInstance() {
        //first check... fast access usually cached in the thread
        if (connectionManager == null) {
            //now pay the cost of synchronize it
            synchronized (JmxConnectionManager.class) {
                //second check... protects against concurrent creations
                if (connectionManager == null) {
                    //finally, create the instance
                    connectionManager = new JmxConnectionManager();
                }
            }
        }
        return connectionManager;
    }

    public static JMXServiceURL jbossRemotingUrl(String address, int port) throws MalformedURLException {
        return new JMXServiceURL(System.getProperty("jmx.service.url",
                "service:jmx:remoting-jmx://" + address + ":" + port));
    }

    public static JMXServiceURL jmxRmiUrl(String address, int port) throws MalformedURLException {
        return new JMXServiceURL("service:jmx:rmi:///jndi/rmi://"+address+":"+port+"/jmxrmi");
    }

    public MBeanServerConnection getConnection(JMXServiceURL url) throws IOException {
        ConnectionWrapper wrapper = new ConnectionWrapper();
        ConnectionWrapper existing = connectionMap.putIfAbsent(url, wrapper);
        if (existing != null) {
            wrapper = existing;
        }
        wrapper.connect(url);
        return wrapper.connection;
    }

    private class ConnectionWrapper {
        private MBeanServerConnection connection;

        public void connect(JMXServiceURL url) throws IOException {
            if (connection == null) {
                synchronized (this) {
                    if (connection == null) {
                        connection = JMXConnectorFactory.connect(url, null).getMBeanServerConnection();
                    }
                }
            }
        }
    }
}
