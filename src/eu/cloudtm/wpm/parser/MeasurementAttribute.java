package eu.cloudtm.wpm.parser;

public class MeasurementAttribute {
	private int attribute_index;
	private int resource_index;
	private String short_name;
	private String java_type;
	private String value;
	
	
	public MeasurementAttribute() {
		
	}

	public MeasurementAttribute(int attribute,int index,String name,String type,String value){
		this.attribute_index=attribute;
		this.resource_index=index;
		this.short_name=name;
		this.java_type=type;
		this.value=value;
	}
	/*
	 * Metodi di get e set
	 */
	
	public void setAttribute_index(int attribute){
		this.attribute_index=attribute;
	}
	public void setResource_index(int index){
		this.resource_index=index;
	}
	public void setShort_name(String name){
		this.short_name=name;
	}
	public void setJava_type(String type){
		this.java_type=type;
	}
	public void setValue(String value){
		this.value=value;
	}
	
	
	
	public int getAttribute_index(){
	    return this.attribute_index;
	}
	public int getResource_index(){
		return this.resource_index;
	}
	public String getShort_name(){
		return this.short_name;
	}
	public String getJava_type(){
		return this.java_type;
	}
	public String getValue(){
		return this.value;
	}
	
	public String toString(){
		
		return "index: "+this.attribute_index+" resource index: "+this.resource_index+" name: "+this.short_name+" type: "+this.java_type+" value: "+this.value;
		
	}
	
}

