/*
* Copyright (C) 2015 Alexander Verbruggen
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

package be.nabu.libs.cache.resources;

import java.io.IOException;
import java.util.Date;

import be.nabu.libs.cache.api.CacheEntry;
import be.nabu.libs.resources.api.AccessTrackingResource;
import be.nabu.libs.resources.api.FiniteResource;
import be.nabu.libs.resources.api.Resource;
import be.nabu.libs.resources.api.TimestampedResource;

public class ResourceEntry implements CacheEntry {

	private Resource resource;
	private ResourceCache cache;
	private Object key, value;
	private Date valueLastUpdated;

	ResourceEntry(ResourceCache cache, Resource resource) {
		this(cache, resource, null);
	}
	
	ResourceEntry(ResourceCache cache, Resource resource, Object key) {
		this.cache = cache;
		this.resource = resource;
		this.key = key;
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

	@Override
	public Object getKey() throws IOException {
		if (key == null) {
			key = cache.deserializeKey(resource.getName().replaceAll("\\..*$", "")); 
		}
		return key;
	}

	@Override
	public Object getValue() throws IOException {
		if (value == null || valueLastUpdated == null || getLastModified().after(valueLastUpdated)) {
			value = cache.getWithSerializedKey(resource.getName().replaceAll("\\..*$", "")); 
			valueLastUpdated = new Date();
		}
		return value;
	}

}
