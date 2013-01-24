package eu.cloudtm.wpm.parser;
import java.util.ArrayList;
import java.util.StringTokenizer;




public class WPMParser{

	public static Measurement parseLine(String strLine){
		Measurement entry = new Measurement();
		try {
			if(strLine.indexOf("[") == -1){
				return null;
			}
				
			StringTokenizer metadateLineParsed = new StringTokenizer(strLine.substring(0,strLine.indexOf("[")),":");
			entry.setSeq(Long.parseLong(metadateLineParsed.nextToken()));//seq field
			entry.setProbeID(Long.parseLong(metadateLineParsed.nextToken()));//probe id field
			entry.setComponent_ID(metadateLineParsed.nextToken()); //component id field
			entry.setIp(metadateLineParsed.nextToken()); //ip field
			entry.setGroup_ID(metadateLineParsed.nextToken()); //group field
			entry.setProvider_ID(metadateLineParsed.nextToken()); //group field
			entry.setTimestamp(Long.parseLong(metadateLineParsed.nextToken()));//timestamp field
			entry.setResourceType(ResourceType.valueOf(metadateLineParsed.nextToken()));// resource type field
			ArrayList<MeasurementAttribute> attributesList = new ArrayList<MeasurementAttribute>();
			StringTokenizer attributesLineParsed = new StringTokenizer(strLine.substring(strLine.indexOf("[")+1,strLine.lastIndexOf("]")),",");
			//System.out.println(strLine.substring(strLine.indexOf("[")+1,strLine.lastIndexOf("]")));
			while(attributesLineParsed.hasMoreTokens()){
				MeasurementAttribute att = new MeasurementAttribute();
				String aux = attributesLineParsed.nextToken();
				//System.out.println(aux);
				StringTokenizer attribute = new StringTokenizer(aux," ");
				att.setAttribute_index(Integer.parseInt(attribute.nextToken().replace(":", "")));//attribute index field
				String complete_short_name = attribute.nextToken();//short name + resource index field
				complete_short_name = complete_short_name.replaceAll(":", "");
				if(complete_short_name.contains("-")){
					att.setResource_index(Integer.parseInt(complete_short_name.substring(0, complete_short_name.indexOf("-"))));
					att.setShort_name(complete_short_name.substring(complete_short_name.indexOf("-")+1));
				}else{
					att.setResource_index(0);
					att.setShort_name(complete_short_name);
				}
				switch(entry.getResourceType()){
					case CPU : att.setShort_name("CpuPerc."+att.getShort_name());break;
					case MEMORY : att.setShort_name("MemoryInfo."+att.getShort_name());break;
					case DISK : att.setShort_name("fileSystemUsage."+att.getShort_name());break;
					//case NETWORK : att.setShort_name("CpuPerc."+att.getShort_name());break;
				}
				att.setJava_type(attribute.nextToken());//java type field
				att.setValue(attribute.nextToken());// value field
				attributesList.add(att);
			}
			entry.setMeasurementAttributes(attributesList);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("String with error: "+strLine);
		}
		return entry;
	}
}