package eu.cloudtm.wpm.parser;

public class StatsMeasurement {
	private String component_ID;
	private String group_ID;
	private String provider_ID;
	private long timestamp;
	private ResourceType resourceType;
	private String misType;
	private double averageValue;
	private double maxValue;
	private double minValue;

	public String getComponent_ID() {
		return component_ID;
	}

	public void setComponent_ID(String component_ID) {
		this.component_ID = component_ID;
	}

	public String getGroup_ID() {
		return group_ID;
	}

	public void setGroup_ID(String group_ID) {
		this.group_ID = group_ID;
	}

	public String getProvider_ID() {
		return provider_ID;
	}

	public void setProvider_ID(String provider_ID) {
		this.provider_ID = provider_ID;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public ResourceType getResourceType() {
		return resourceType;
	}

	public void setResourceType(ResourceType resourceType) {
		this.resourceType = resourceType;
	}

	public String getMisType() {
		return misType;
	}

	public void setMisType(String misType) {
		this.misType = misType;
	}

	public double getAverageValue() {
		return averageValue;
	}

	public void setAverageValue(double averageValue) {
		this.averageValue = averageValue;
	}

	public double getMaxValue() {
		return maxValue;
	}

	public void setMaxValue(double maxValue) {
		this.maxValue = maxValue;
	}

	public double getMinValue() {
		return minValue;
	}

	public void setMinValue(double minValue) {
		this.minValue = minValue;
	}

	@Override
	public String toString() {
		return component_ID + " " + group_ID + " " + provider_ID + " "
				+ timestamp + " " + resourceType + " " + misType + " "
				+ averageValue + " " + maxValue + " " + minValue;
	}

}
