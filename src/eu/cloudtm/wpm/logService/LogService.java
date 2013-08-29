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
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.RemoteException;
import java.util.Properties;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;

import javax.net.ssl.SSLServerSocketFactory;

import eu.cloudtm.wpm.Utils;
import org.apache.log4j.Logger;

/**
* @author Roberto Palmieri
* @author Sebastiano Peluso
*/
public class LogService {
	
	private final static Logger log = Logger.getLogger(LogService.class);
	private final static boolean INFO = log.isInfoEnabled();
	public static final String PROPERTY_FILE = "config/log_service.config";

	static int port_num;
	static final int filesize = 2048;

	public static void main(String[] args) throws RemoteException {
		//start analyzer thread 
		new LogServiceAnalyzer();
		
		//start ack thread
		//new LogServiceAck();
		
		//receive zip + check file
		try{
			loadParametersFromRegistry();
			//ServerSocket servsock = new ServerSocket(port_num);
			//-------
			ServerSocket servsock = getServer();
			//-------
			while (true) {
				if(INFO)
					log.info("Log Service Waiting...");
				Socket sock = null;
				DataInputStream dis = null;
                try {
                    sock = servsock.accept();
                    dis = new DataInputStream(sock.getInputStream());
                    //receive Zip file
                    File zipFile = receiveFile(dis);
                    //receive Check file
                    File checkFile = receiveFile(dis);
                    checkZipFile(checkFile,zipFile);
                } finally {
                    Utils.safeClose(dis);
                    Utils.safeClose(sock);
                }
				if(INFO)
					log.info("Now I can process...");
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	private static void loadParametersFromRegistry(){
    	Properties props = Utils.loadProperties(PROPERTY_FILE);
		port_num = Integer.parseInt(props.getProperty("Port_number"));
    }
	public static ServerSocket getServer() throws Exception {
		SSLServerSocketFactory sslserversocketfactory = (SSLServerSocketFactory) SSLServerSocketFactory.getDefault();
        return sslserversocketfactory.createServerSocket(port_num);
	}
	
	private static void checkZipFile(File checkFile, File zipFile){
		if(INFO)
			log.info("checking files...");
        BufferedReader br = null;
        CheckedInputStream check = null;
        BufferedInputStream in_zip = null;
        try{
		    br = new BufferedReader(new InputStreamReader(new FileInputStream(checkFile)));
		    check = new CheckedInputStream(new FileInputStream(zipFile), new Adler32());
		    in_zip = new BufferedInputStream(check);
		    while (in_zip.read() != -1);
		    String strLine;
		    if((strLine = br.readLine()) != null){
		    	try{
		    		long fileCheck_cks = Long.parseLong(strLine);
		    		long fileZip_cks = check.getChecksum().getValue();
		    		if(INFO)
		    			log.info(fileCheck_cks+"-"+fileZip_cks);
		    		if(fileCheck_cks == fileZip_cks){
		    			copyfile(zipFile,new File("log/ls_processed/"+zipFile.getName()));
			    		checkFile.delete();
			    		zipFile.delete();
		    		}
		    	}catch(Exception e){
		    		e.printStackTrace();
		    		throw new RuntimeException("Bad check file");
		    	}
		    }
		}catch(Exception ex){
			ex.printStackTrace();
		} finally {
            Utils.safeCloseAll(br, check, in_zip);
        }
    }
	
	private static File receiveFile(DataInputStream dis){
		try{
			return Utils.receiveFile(dis);
		}catch(Exception e){
			e.printStackTrace();
		}
        return null;
	}
	
	private static void copyfile(File f1, File f2){
		try{
			Utils.copyFile(f1, f2);
		}catch(FileNotFoundException ex){
			System.out.println(ex.getMessage() + " in the specified directory.");
			System.exit(0);
		} catch (IOException e) {
            //no-op
        }
    }
}
