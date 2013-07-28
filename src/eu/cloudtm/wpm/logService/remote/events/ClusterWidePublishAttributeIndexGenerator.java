package eu.cloudtm.wpm.logService.remote.events;

import java.util.HashMap;

import eu.cloudtm.wpm.parser.ResourceType;


/*
 * @author Sebastiano Peluso
 * 
 * This class is not thread-safe
 */
public class ClusterWidePublishAttributeIndexGenerator {
	
	
	private static ClusterWidePublishAttributeIndexGenerator generator = new ClusterWidePublishAttributeIndexGenerator();
	
	private HashMap<String, Integer> CPUstats;
	private int nextCPUId;
	private HashMap<String, Integer> DISKstats;
	private int nextDISKId;
	private HashMap<String, Integer> NETWORKstats;
	private int nextNETWORKId;
	private HashMap<String, Integer> JMXstats;
	private int nextJMXId;
	private HashMap<String, Integer> MEMORYstats;
	private int nextMEMORYId;
    private HashMap<String, Integer> FENIXstats;
    private int nextFENIXId;
	
	private ClusterWidePublishAttributeIndexGenerator(){
		
		CPUstats = new HashMap<String, Integer>();
		DISKstats = new HashMap<String, Integer>();
		NETWORKstats = new HashMap<String, Integer>();
		JMXstats = new HashMap<String, Integer>();
		MEMORYstats = new HashMap<String, Integer>();
        FENIXstats = new HashMap<String, Integer>();
		
		nextCPUId = 0;
		nextDISKId = 0;
		nextNETWORKId = 0;
		nextJMXId = 0;
		nextMEMORYId = 0;
		nextFENIXId = 0;
		
	}
	
	public static ClusterWidePublishAttributeIndexGenerator getInstance(){
		
		return generator;
		
	}
	
	
	public int getId(String attributeName, ResourceType rt){

		Integer id;
		
		if(ResourceType.CPU.equals(rt)){
			
			id = CPUstats.get(attributeName);
			if(id == null){
				id = new Integer(nextCPUId++);
				CPUstats.put(attributeName, id);
			}
			
			return id;
		}
		else if(ResourceType.DISK.equals(rt)){
			
			id = DISKstats.get(attributeName);
			if(id == null){
				id = new Integer(nextDISKId++);
				DISKstats.put(attributeName, id);
			}
			
			return id;
		}
		else if(ResourceType.NETWORK.equals(rt)){
		
			id = NETWORKstats.get(attributeName);
			if(id == null){
				id = new Integer(nextNETWORKId++);
				NETWORKstats.put(attributeName, id);
			}
			
			return id;
		}
		else if(ResourceType.JMX.equals(rt)){
			
			id = JMXstats.get(attributeName);
			if(id == null){
				id = new Integer(nextJMXId++);
				JMXstats.put(attributeName, id);
			}
			
			return id;
		}
		else if(ResourceType.MEMORY.equals(rt)){
			
			id = MEMORYstats.get(attributeName);
			if(id == null){
				id = new Integer(nextMEMORYId++);
				MEMORYstats.put(attributeName, id);
			}
			
			return id;
		}
        else if (rt == ResourceType.FENIX) {
            id = FENIXstats.get(attributeName);
            if (id == null) {
                id = nextFENIXId++;
                FENIXstats.put(attributeName, id);
            }
            return id;
        }
		else{
			return -1;
		}
		
		

	}

}
