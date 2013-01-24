package eu.cloudtm.wpm.logService.remote.events;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import eu.cloudtm.wpm.parser.ResourceType;

public class PublishAttribute<T> implements Externalizable{
	
	private ResourceType resourceType;
	private int resourceIndex;
	
	private String name;
	private T value;
	
	public PublishAttribute() {
		
	}

	public PublishAttribute(ResourceType resourceType, int resourceIndex, String name, T value){
		
		this.resourceType = resourceType;
		this.resourceIndex = resourceIndex;
		this.name=name;
		this.value=value;
	}
	
	public T getValue(){
		
		return this.value;
	}
	
	public String getName(){
		return this.name;
	}
	
	

	public ResourceType getResourceType() {
		return resourceType;
	}

	public void setResourceType(ResourceType resourceType) {
		this.resourceType = resourceType;
	}

	public int getResourceIndex() {
		return resourceIndex;
	}

	public void setResourceIndex(int resourceIndex) {
		this.resourceIndex = resourceIndex;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setValue(T value) {
		this.value = value;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		
		this.resourceType = ResourceType.valueOf(ResourceType.class, in.readUTF());
		this.resourceIndex = in.readInt();
		this.name = in.readUTF();
		this.value = (T) in.readObject();
		
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		
		out.writeUTF(this.resourceType.name());
		out.writeInt(this.resourceIndex);
		out.writeUTF(this.name);
		out.writeObject(this.value);
		
	}

}
