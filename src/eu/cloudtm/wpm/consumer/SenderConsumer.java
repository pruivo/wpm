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
 
package eu.cloudtm.wpm.consumer;

import java.io.BufferedInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import javax.net.ssl.SSLSocketFactory;

import eu.cloudtm.wpm.Utils;
import org.apache.log4j.Logger;

/**
* @author Roberto Palmieri
* @author Sebastiano Peluso
*/
public class SenderConsumer implements Runnable{
	
	
	private final static Logger log = Logger.getLogger(SenderConsumer.class);
	private final static boolean INFO = log.isInfoEnabled();
	
	private String logService_addr;
	private int logService_port;
	private long timeout;
	
	public SenderConsumer(String logServiceAddr, int logServicePort,long period){
		logService_addr = logServiceAddr;
		logService_port = logServicePort;
		timeout = period;
		Thread sender = new Thread(this,"Consumer Sender");
		sender.start();
	}

	@Override
	public void run() {
		while(true){
			try {
				Thread.sleep(timeout);
			} catch (InterruptedException e) {
				e.printStackTrace();
                Thread.currentThread().interrupt();
                return;
			}
			try {
				if(INFO)
					log.info("Consumer Sender Thread active!!");
				File active_folder = new File("log/active");
				if(active_folder.isDirectory()){
					for(File activeFile : active_folder.listFiles()){
						if(!activeFile.getName().endsWith(".ready"))
							continue;
                        DataOutputStream dos = null;
                        Socket sock = null;
						try{
							//now check if exist .zip and .check files
							File zipFile = new File("log/active/"+activeFile.getName().substring(0,activeFile.getName().indexOf(".ready"))+".zip");
							//System.out.println(zipFile.getName()+" is a file: "+zipFile.isFile());
							File checkFile = new File("log/active/"+activeFile.getName().substring(0,activeFile.getName().indexOf(".ready"))+".check");
							//System.out.println(checkFile.getName()+" is a file: "+checkFile.isFile());
							if(zipFile != null && zipFile.isFile() && checkFile != null && checkFile.isFile()){
								//send zip and check file to log Service
								SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
								sock = factory.createSocket(logService_addr, logService_port);
								dos = new DataOutputStream(sock.getOutputStream());
								sendFile(zipFile,dos);
								sendFile(checkFile, dos);
					            //delete ready file for make the comunication simple
					            
					            if(!activeFile.delete()){
					            	
					            	if(INFO)
					            		log.info("READY file not deleted!! "+activeFile);
					            }
					        }
						}catch(Exception e){
							e.printStackTrace();
						} finally {
                            Utils.safeClose(dos);
                            Utils.safeClose(sock);
                        }
                    }
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	private void sendFile(File file, DataOutputStream dos){
		try {
			Utils.sendFile(file, dos);
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
}
