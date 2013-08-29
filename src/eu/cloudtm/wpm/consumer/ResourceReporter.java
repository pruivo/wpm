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

import java.io.*;
import java.util.zip.Adler32;
import java.util.zip.CheckedOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import eu.cloudtm.wpm.Utils;
import org.apache.log4j.Logger;

import eu.cloudtm.wpm.hw_probe.NetworkResourceProbe;
import eu.reservoir.monitoring.core.Measurement;
import eu.reservoir.monitoring.core.ProbeValue;
import eu.reservoir.monitoring.core.Reporter;
import eu.reservoir.monitoring.core.plane.InfoPlane;
import eu.reservoir.monitoring.distribution.ConsumerMeasurementWithMetaData;
import eu.reservoir.monitoring.distribution.ConsumerMeasurementWithMetadataAndProbeName;

/**
* @author Roberto Palmieri
* @author Sebastiano Peluso
*/
public class ResourceReporter implements Reporter {
	
	private final static Logger log = Logger.getLogger(ResourceReporter.class);
	private final static boolean INFO = log.isInfoEnabled();
	
	private InfoPlane infoModel;
	private String id_consumer;
	private long lastTimestamp;
	private long timeToRefresh;
	
	public ResourceReporter(InfoPlane infoPlane,String id_cons,long refresh_period) {
		infoModel = infoPlane;
		id_consumer = id_cons;
		timeToRefresh = refresh_period;
	}
	
	public void report(Measurement m) {
		//String probeName = (String)infoModel.lookupProbeInfo(m.getProbeID(), "name");
		//System.out.print("Received msg from probe"+probeName + " => ");
		long currentTimestamp = System.currentTimeMillis();
		File logFile = new File("log/stat_"+id_consumer+".log");
		try{
			appendMeasurement(m, logFile);
			if(currentTimestamp - lastTimestamp >= timeToRefresh){
				if(INFO)
					log.info("Generating zip file...");
				byte [] logFileByteArray  = new byte [(int)logFile.length()];
				BufferedInputStream bis = new BufferedInputStream(new FileInputStream(logFile));
				//Create zip file to store
				File logFileZip = new File("log/active/"+(logFile.getName().substring(0,logFile.getName().lastIndexOf(".log")))+"_"+lastTimestamp+"_"+currentTimestamp+".log.zip");
				CheckedOutputStream checksum = new CheckedOutputStream(new FileOutputStream(logFileZip), new Adler32());
				ZipOutputStream outZip = new ZipOutputStream(checksum);
				ZipEntry entry = new ZipEntry(logFileZip.getName().substring(0,logFileZip.getName().lastIndexOf(".zip")));
				outZip.putNextEntry(entry);
	            bis.read(logFileByteArray, 0, logFileByteArray.length);
	            outZip.write(logFileByteArray, 0, logFileByteArray.length);
                outZip.closeEntry();
	            Utils.safeCloseAll(bis, outZip);
	            if(INFO){
	            	log.info("done!");
	            	log.info("Zip file stored: "+logFile.getPath());
	            }
	            File checkFile = new File("log/active/"+logFileZip.getName().substring(0,logFileZip.getName().lastIndexOf(".zip"))+".check");
	            BufferedWriter checkFile_writer = new BufferedWriter(new FileWriter(checkFile));
	            checkFile_writer.write(Long.toString(checksum.getChecksum().getValue()));
	            Utils.safeClose(checkFile_writer);
	            if(INFO)
	            	log.info("Check file stored: "+checkFile.getPath());
	            File readyFile = new File("log/active/"+logFileZip.getName().substring(0,logFileZip.getName().lastIndexOf(".zip"))+".ready");
	            if(!readyFile.createNewFile())
	            	throw new RuntimeException("Error while creating ready file");
	            if(INFO)
	            	log.info("Ready file stored: "+readyFile.getPath());
				lastTimestamp = currentTimestamp;
				logFile.delete();
			}
			//System.out.print("Fine elaborazione");
		}catch(Exception ex){
			ex.printStackTrace();
			//delete file if is bigger than 100Mb
			try{
				if(logFile.length() > 100000000)
					logFile.delete();
			}catch(Exception e){
				e.printStackTrace();
			}
			System.exit(0);
		}
	}
	public static String formatMeasurement(ConsumerMeasurementWithMetadataAndProbeName m){
		return ""+m.getSequenceNo()+":"+m.getProbeID()+":"+m.getProbeName()+":"+m.getTimestamp()+":"+m.getType()+m.getValues()+"\n";
	}
	public static String formatMeasurementWithName(ConsumerMeasurementWithMetaData m, InfoPlane infoModel2){
		String probeName = (String)infoModel2.lookupProbeInfo(m.getProbeID(), "name");
		//System.out.println(m.getValues());
		String values = "[";
		for(ProbeValue pv : m.getValues()){
			String name_att = (String)infoModel2.lookupProbeAttributeInfo(m.getProbeID(), pv.getField(), "name");
			values += pv.getField()+": "+name_att+": "+pv.getType()+" "+pv.getValue()+", ";
		}
		values = values.substring(0,values.length()-1)+"]";
		
		return ""+m.getSequenceNo()+":"+m.getProbeID()+":"+probeName+":"+m.getTimestamp()+":"+m.getType()+values+"\n";
	}

    private void appendMeasurement(Measurement measurement, File file) throws IOException {
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new FileWriter(file, true));

            //out.write("Name:"+probeName+"Value:"+m.toString()+"\n");
            if (measurement instanceof ConsumerMeasurementWithMetadataAndProbeName) {
                ConsumerMeasurementWithMetadataAndProbeName cm = (ConsumerMeasurementWithMetadataAndProbeName) measurement;
                //out.write(m.toString()+"\n");
                out.write(formatMeasurement(cm));
            } else if (measurement instanceof ConsumerMeasurementWithMetaData) {
                ConsumerMeasurementWithMetaData cm = (ConsumerMeasurementWithMetaData) measurement;
                //out.write(ng()+"\n");
                out.write(formatMeasurementWithName(cm, infoModel));
            } else {
                throw new RuntimeException("Unsupported measurement message");
            }
            if (INFO)
                log.info("File updated!!");
        } finally {
            Utils.safeClose(out);
        }

    }
}
