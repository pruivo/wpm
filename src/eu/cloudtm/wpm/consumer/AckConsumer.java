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

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import javax.net.ssl.SSLServerSocketFactory;

import eu.cloudtm.wpm.Utils;
import org.apache.log4j.Logger;

/*
* @author Roberto Palmieri
* @author Sebastiano Peluso
*/
public class AckConsumer implements Runnable{

	private final static Logger log = Logger.getLogger(AckConsumer.class);
	private final static boolean INFO = log.isInfoEnabled();

	private int port_num;
	private static final int filesize = 1024;

	public AckConsumer(int consumerAckPort){
		port_num = consumerAckPort;
		Thread consumer_ack = new Thread(this,"Consumer Ack Thread");
		consumer_ack.start();
	}

	@Override
	public void run() {
		ServerSocket servsock;
		try {
			servsock = getServer();
		} catch (Exception e1) {
			e1.printStackTrace();
            return;
		}
		while(true){
            DataOutputStream dos = null;
            DataInputStream dis = null;
            Socket sock = null;
			try {
				if(INFO)
					log.info("Consumer Ack Thread Waiting on port..."+port_num);
				sock = servsock.accept();
				if(INFO)
					log.info("Consumer Ack Thread accepted connection...");
				dos = new DataOutputStream (sock.getOutputStream());
				dis = new DataInputStream(sock.getInputStream());
				//receive ack file
				File ackFile = receiveFile(dis);
				if(INFO)
					log.info("Ack File received "+ackFile.getName());
				File readyFile = new File("log/active/"+ackFile.getName().substring(0,ackFile.getName().indexOf(".ack"))+".ready");
				if(readyFile.isFile()){
					//readyFile.renameTo(new File("log/active/"+readyFile.getName().substring(0,readyFile.getName().indexOf(".ready"))+".del"));
					readyFile.delete();
				}
				File checkFile = new File("log/active/"+ackFile.getName().substring(0,ackFile.getName().indexOf(".ack"))+".check");
				if(checkFile.isFile()){
					checkFile.delete();
				}
				File zipFile = new File("log/active/"+ackFile.getName().substring(0,ackFile.getName().indexOf(".ack"))+".zip");
				if(zipFile.isFile()){
					zipFile.delete();
				}
				sendMsg("ACK", dos);

			} catch (IOException e) {
				e.printStackTrace();
				sendMsg("NACK",dos);
			} finally {
                Utils.safeCloseAll(dis, dos);
                Utils.safeClose(sock);
            }
        }
	}
	private void sendMsg(String strMsg, DataOutputStream dos){
		try{
		    dos.writeInt(strMsg.getBytes().length);
		    dos.flush();
		    dos.write(strMsg.getBytes());
			dos.flush();
		}catch(Exception e){
			e.printStackTrace();
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

	public ServerSocket getServer() throws Exception {
		SSLServerSocketFactory sslserversocketfactory = (SSLServerSocketFactory) SSLServerSocketFactory.getDefault();
        return sslserversocketfactory.createServerSocket(port_num);
	}
}
