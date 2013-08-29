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
 
package eu.cloudtm.wpm.producer;


import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import eu.cloudtm.wpm.Utils;
import org.apache.log4j.Logger;

import eu.cloudtm.wpm.consumer.AckConsumer;

/**
* @author Roberto Palmieri
* @author Sebastiano Peluso
*/

public class HwResourceMain {
	
	private final static Logger log = Logger.getLogger(HwResourceMain.class);
	private final static boolean INFO = log.isInfoEnabled();
	
	static String hostName;
	static String Consumer_DP_IP_Address;
	static int Consumer_DP_port_number;
	static String Consumer_IP_IP_Address;
	static int Consumer_IP_local_port_number;
	static int Consumer_IP_remote_port_number;
	public static void main(String[] args) {
		loadParametersFromRegistry();
		// allocate a Controller 
		ResourcesController controller = new ResourcesController(Consumer_DP_IP_Address,Consumer_DP_port_number,Consumer_DP_IP_Address,Consumer_IP_local_port_number,Consumer_IP_remote_port_number,hostName);
		// activate the controller
		do{
			if(INFO)
				log.info("While cycle pre activate");
			controller.activateControl();
			if(INFO)
				log.info("While cycle post activate "+controller.isRunning());
		}while(!controller.isRunning());
		
		
	}
	private static void loadParametersFromRegistry(){
    	Properties props = Utils.loadProperties(ResourcesController.PROPERTY_FILE);
		hostName = props.getProperty("hostName");
		Consumer_DP_IP_Address = props.getProperty("Consumer_DP_IP_Address");
		Consumer_DP_port_number = Integer.parseInt(props.getProperty("Consumer_DP_port_number"));
		Consumer_IP_IP_Address = props.getProperty("Consumer_IP_IP_Address");
		Consumer_IP_remote_port_number = Integer.parseInt(props.getProperty("Consumer_IP_remote_port_number"));
		Consumer_IP_local_port_number = Integer.parseInt(props.getProperty("Consumer_IP_local_port_number"));
    }
}
