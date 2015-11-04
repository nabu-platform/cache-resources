package be.nabu.libs.cache.resources;

import java.util.Date;

public class ResourceCacheEntry implements Comparable<ResourceCacheEntry> {
	
	private String key;
	private Class<?> valueClass;
	private Date lastAccessed = new Date();
	private Date created = new Date();
	private Class<?> keyClass;
	
	public ResourceCacheEntry(String key, Class<?> keyClass, Class<?> valueClass) {
		this.key = key;
		this.keyClass = keyClass;
		this.valueClass = valueClass;
	}
	
	public Class<?> getValueClass() {
		return valueClass;
	}
	public Class<?> getKeyClass() {
		return keyClass;
	}
	public Date getLastAccessed() {
		return lastAccessed;
	}
	public void accessed() {
		lastAccessed = new Date();
	}
	public String getKey() {
		return key;
	}
	public Date getCreated() {
		return created;
	}

	@Override
	public int compareTo(ResourceCacheEntry o) {
		return lastAccessed.compareTo(o.lastAccessed);
	}

}
