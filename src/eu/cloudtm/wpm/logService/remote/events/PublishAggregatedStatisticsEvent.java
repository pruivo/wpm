package eu.cloudtm.wpm.logService.remote.events;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;

import eu.cloudtm.wpm.parser.ResourceType;

public class PublishAggregatedStatisticsEvent extends PublishEvent {

	PublishMeasurement cpuValues;   
    PublishMeasurement memoryValues;
    PublishMeasurement jmxValues;
    PublishMeasurement networkValues;
    PublishMeasurement diskValues;
    PublishMeasurement fenixValues;
    
    
    public PublishAggregatedStatisticsEvent(){
    		
    	
    }
    
    public void addMeasure(long timestamp, ResourceType rt, PublishAttribute[] attributes){

    	if(attributes != null){

    		PublishMeasurement pm = new PublishMeasurement(timestamp);

    		if(ResourceType.CPU.equals(rt)){
    			cpuValues = pm;
    		}
    		else if(ResourceType.MEMORY.equals(rt)){
    			memoryValues = pm;
    		}
    		else if(ResourceType.JMX.equals(rt)){
    			jmxValues = pm;
    		}
    		else if(ResourceType.NETWORK.equals(rt)){
    			networkValues = pm;
    		}
    		else if(ResourceType.DISK.equals(rt)){
    			diskValues = pm;
    		} else if (rt == ResourceType.FENIX) {
                fenixValues = pm;
            }

    		for(int i=0; i < attributes.length; i++){
    				
    			pm.addMeasure(attributes[i]);

    		}

    	}

    }
    
    
    public PublishMeasurement getPublishMeasurement(ResourceType rt){
    	
    	if(ResourceType.CPU.equals(rt)){
			return cpuValues;
		}
		else if(ResourceType.MEMORY.equals(rt)){
			return memoryValues;
		}
		else if(ResourceType.JMX.equals(rt)){
			return jmxValues;
		}
		else if(ResourceType.NETWORK.equals(rt)){
			return networkValues;
		}
		else if(ResourceType.DISK.equals(rt)){
			return diskValues;
		} else if (rt == ResourceType.FENIX) {
            return fenixValues;
        }
    	
    	
    	return null;
    	
    }

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		cpuValues = (PublishMeasurement) in.readObject();
		memoryValues = (PublishMeasurement) in.readObject();
		jmxValues = (PublishMeasurement) in.readObject();
		networkValues = (PublishMeasurement) in.readObject();
		diskValues = (PublishMeasurement) in.readObject();
        fenixValues = (PublishMeasurement) in.readObject();

	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(cpuValues);
		out.writeObject(memoryValues);
		out.writeObject(jmxValues);
		out.writeObject(networkValues);
		out.writeObject(diskValues);
		out.writeObject(fenixValues);

	}

}
