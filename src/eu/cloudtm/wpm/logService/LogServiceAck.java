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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Properties;

import javax.net.ssl.SSLSocketFactory;

import eu.cloudtm.wpm.Utils;
import org.apache.log4j.Logger;

/*
* @author Roberto Palmieri
*/
public class LogServiceAck implements Runnable{
	
	private final static Logger log = Logger.getLogger(LogServiceAck.class);
	private final static boolean INFO = log.isInfoEnabled();
	
	private int consumer_port;
	private long timeout;
	
	public LogServiceAck(){
		loadParametersFromRegistry();
		Thread ack_thread = new Thread(this,"Log Service Ack");
		ack_thread.start();
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
				if(INFO)
					log.info("Running Log Service Ack Thread!!");
				File active_folder = new File("log/ls_worked");
				if(active_folder.isDirectory()){
					for(File activeFile : active_folder.listFiles()){
						if(!activeFile.getName().endsWith(".ack"))
							continue;
                        Socket sock = null;
                        DataOutputStream dos = null;
                        DataInputStream dis = null;
						try {
							//take the ip on the filename sent by consumer
							//activeFile contains the IP to send the ack file
							//stat_127.0.0.1_0_1338842406981.log
							String IP_consumer = "";
							if(activeFile.getName().startsWith("stat_")){
								String filename_no_stat = activeFile.getName().substring(5);
								IP_consumer = filename_no_stat.substring(0, filename_no_stat.indexOf("_"));
							}
							//String IP_consumer = activeFile.
							SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
							sock = factory.createSocket(IP_consumer, consumer_port);
                            dos = new DataOutputStream(sock.getOutputStream());
							dis = new DataInputStream(sock.getInputStream());
							
							sendFile(activeFile,dos);
							if(INFO)
								log.info("Ack sent for file "+activeFile.getName());
							String msg = receiveAck(dis);
							if(INFO)
								log.info("Ack received "+msg);
							if(msg.equals("ACK"))
								activeFile.delete();
							else if(msg.equals("NACK")){
								if(INFO)
									log.info("NACK received");
							}	
						} catch (IOException e) {
							e.printStackTrace();
						} finally {
                            Utils.safeCloseAll(dis, dos);
                            Utils.safeClose(sock);
                        }
                    }
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	private void loadParametersFromRegistry(){
    	Properties props = Utils.loadProperties(LogService.PROPERTY_FILE);
		consumer_port = Integer.parseInt(props.getProperty("Consumer_ack_port_number"));
		timeout = Long.parseLong(props.getProperty("AckThreadTimeout"));
    }
	
	private void sendFile(File file,DataOutputStream dos){
		try {
			Utils.sendFile(file, dos);
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
	
	private static String receiveAck(DataInputStream dis){
		try{
			byte [] msgInByte = new byte [1024];
		    int count = 0;
		    int sizeOfMsg = dis.readInt();
		    count = dis.read(msgInByte, 0, sizeOfMsg);
            return new String(msgInByte,0,count);
		}catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}
}
