package be.nabu.libs.cache.resources;

import java.util.Date;

import be.nabu.libs.cache.api.CacheEntry;
import be.nabu.libs.resources.api.AccessTrackingResource;
import be.nabu.libs.resources.api.FiniteResource;
import be.nabu.libs.resources.api.Resource;
import be.nabu.libs.resources.api.TimestampedResource;

public class ResourceEntry implements CacheEntry {

	private Resource resource;

	public ResourceEntry(Resource resource) {
		this.resource = resource;
	}
	
	@Override
	public long getSize() {
		return resource.getName().replaceAll("\\..*$", "").length() + ((FiniteResource) resource).getSize();
	}

	@Override
	public Date getLastAccessed() {
		return resource instanceof AccessTrackingResource ? ((AccessTrackingResource) resource).getLastAccessed() : null;
	}

	@Override
	public Date getLastModified() {
		return resource instanceof TimestampedResource ? ((TimestampedResource) resource).getLastModified() : null;
	}

}
