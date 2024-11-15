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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.libs.cache.api.CacheEntry;
import be.nabu.libs.cache.api.CacheRefresher;
import be.nabu.libs.cache.api.CacheTimeoutManager;
import be.nabu.libs.cache.api.CacheWithHash;
import be.nabu.libs.cache.api.DataSerializer;
import be.nabu.libs.cache.api.ExplorableCache;
import be.nabu.libs.cache.api.LimitedCache;
import be.nabu.libs.resources.ResourceReadableContainer;
import be.nabu.libs.resources.ResourceWritableContainer;
import be.nabu.libs.resources.api.AccessTrackingResource;
import be.nabu.libs.resources.api.FiniteResource;
import be.nabu.libs.resources.api.ManageableContainer;
import be.nabu.libs.resources.api.ReadableResource;
import be.nabu.libs.resources.api.Resource;
import be.nabu.libs.resources.api.ResourceContainer;
import be.nabu.libs.resources.api.TimestampedResource;
import be.nabu.libs.resources.api.WritableResource;
import be.nabu.utils.codec.TranscoderUtils;
import be.nabu.utils.codec.impl.Base64Decoder;
import be.nabu.utils.codec.impl.Base64Encoder;
import be.nabu.utils.codec.impl.GZIPDecoder;
import be.nabu.utils.codec.impl.GZIPEncoder;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.ReadableContainer;
import be.nabu.utils.io.api.WritableContainer;
import be.nabu.utils.io.containers.chars.HexReadableCharContainer;

public class ResourceCache implements ExplorableCache, LimitedCache, CacheWithHash {

	private Logger logger = LoggerFactory.getLogger(getClass());
	private long maxEntrySize;
	private ManageableContainer<?> container;
	private CacheRefresher refresher;
	private String extension = "bin";
	private long maxCacheSize;
	private DataSerializer<?> keySerializer, valueSerializer;
	private CacheTimeoutManager timeoutManager;
	// do we want to hash the data itself or the metadata?
	private boolean hashMetadata = true;
	
	public ResourceCache(ManageableContainer<?> container, long maxEntrySize, long maxCacheSize, CacheRefresher cacheRefresher, CacheTimeoutManager timeoutManager) {
		this.container = container;
		this.maxEntrySize = maxEntrySize;
		this.maxCacheSize = maxCacheSize;
		this.timeoutManager = timeoutManager;
		this.refresher = cacheRefresher;
	}
	
	@Override
	public synchronized boolean put(Object key, Object value) throws IOException {
		String serializedKey = serializeKey(key);
		return put(serializedKey, value);
	}

	private synchronized boolean put(String serializedKey, Object value) throws IOException {
		Resource child = container.getChild(serializedKey + "." + extension);
		if (child == null) {
			child = container.create(serializedKey + "." + extension, "application/octet-stream");
		}
		try {
			serializeValue(serializedKey, value, new ResourceWritableContainer((WritableResource) child));
		}
		catch (Exception e) {
			logger.error("Could not store data in: " + child, e);
			// if we fail to serialize the value (e.g. because it is too big), we must delete the resource
			container.delete(serializedKey + "." + extension);
			return false;
		}
		prune();
		return true;
	}

	public List<Resource> getResources() {
		List<Resource> resources = new ArrayList<Resource>();
		for (Resource resource : container) {
			resources.add(resource);
		}
		return resources;
	}
	
	@Override
	public synchronized void refresh() throws IOException {
		if (refresher != null) {
			for (Resource resource : container) {
				refresh(resource);
			}
		}
	}

	private synchronized boolean refresh(Resource resource) throws IOException {
		// strip the extension again
		String keyValue = resource.getName().replaceAll("\\..*$", "");
		Object deserializedKey = deserializeKey(keyValue);
		// it can be null if there is not enough metadata to deserialize the key, in this case it is skipped for refresh
		if (deserializedKey != null) {
			Object refreshed = refresher.refresh(deserializedKey);
			if (refreshed != null) {
				return put(keyValue, refreshed);
			}
		}
		return false;
	}
	
	@Override
	public synchronized void prune() throws IOException {
		long totalSize = getCurrentSize();
		if (maxCacheSize > 0 && totalSize > maxCacheSize) {
			List<Resource> list = getResources();
			// order by last accessed date, oldest is first
			Collections.sort(list, new Comparator<Resource>() {
				@Override
				public int compare(Resource o1, Resource o2) {
					if (o1 instanceof AccessTrackingResource && o2 instanceof AccessTrackingResource) {
						return ((AccessTrackingResource) o1).getLastAccessed().compareTo(((AccessTrackingResource) o2).getLastAccessed());
					}
					else if (o1 instanceof TimestampedResource && o2 instanceof TimestampedResource) {
						return ((TimestampedResource) o1).getLastModified().compareTo(((TimestampedResource) o2).getLastModified());
					}
					return o1.getName().compareTo(o2.getName());
				}
			});
			// we first prune the entries that have not been used for the longest time
			// then we keep pruning as long as they are timed out
			while(!list.isEmpty()) {
				Resource entryToDelete = list.remove(0);
				// if we have gone over size, remove
				if (totalSize > maxCacheSize) {
					String keyValue = entryToDelete.getName().replaceAll("\\..*$", "");
					long sizeToRemove = keyValue.length() + ((FiniteResource) entryToDelete).getSize();
					container.delete(entryToDelete.getName());
					totalSize -= sizeToRemove;
				}
				else {
					break;
				}
			}
		}
	}

	@Override
	public void refresh(Object key) throws IOException {
		String serializedKey = serializeKey(key);
		Resource child = container.getChild(serializedKey + "." + extension);
		if (child != null) {
			refresh(child);
		}
	}
	
	@Override
	public Object get(Object key) throws IOException {
		String serializedKey = serializeKey(key);
		try {
			return getWithSerializedKey(serializedKey);
		}
		catch (Exception e) {
			logger.error("Could not retrieve data", e);
			// if we fail to deserialize the value because of some I/O issues, we must delete the backing resource
			synchronized(this) {
				container.delete(serializedKey + "." + extension);
			}
			return null;
		}
	}

	Object getWithSerializedKey(String serializedKey) throws IOException {
		Resource child = container.getChild(serializedKey + "." + extension);
		if (child == null) {
			// it could be that the entry does not exist or that it is still being added (e.g. the resource exists but not yet the value mapping)
			// it is however a cache so best effort
			return null;
		}
		// either we use access based timeouts and the last accessed is too old
		else if (timeoutManager != null && timeoutManager.isTimedOut(this, new ResourceEntry(this, child))) {
			// if we can't refresh the child, remove it
			if (refresher == null || !refresh(child)) {
				container.delete(child.getName());
				child = null;
			}
		}
		return child == null ? null : deserializeValue(new ResourceReadableContainer((ReadableResource) child));
	}

	@Override
	public void clear(Object key) throws IOException {
		String serializedKey = serializeKey(key);
		Resource child = container.getChild(serializedKey + "." + extension);
		if (child != null) {
			container.delete(serializedKey + "." + extension);
		}
	}

	@Override
	public synchronized void clear() throws IOException {
		ResourceContainer<?> parent = container.getParent();
		if (!(parent instanceof ManageableContainer)) {
			throw new IllegalArgumentException("Can not manipulate the parent container");
		}
		String name = container.getName();
		((ManageableContainer<?>) parent).delete(name);
		container = (ManageableContainer<?>) ((ManageableContainer<?>) parent).create(name, Resource.CONTENT_TYPE_DIRECTORY);
	}

	@Override
	public long getCurrentSize() {
		long totalSize = 0l;
		for (Resource entry : container) {
			String keyValue = entry.getName().replaceAll("\\..*$", "");
			totalSize += ((FiniteResource) entry).getSize() + keyValue.length();
		}
		return totalSize;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected String serializeKey(Object key) throws IOException {
		DataSerializer serializer = getKeySerializer();
		if (serializer == null) {
			throw new IllegalArgumentException("No serializer found for the key");
		}
		ByteBuffer buffer = IOUtils.newByteBuffer();
		Base64Encoder transcoder = new Base64Encoder();
		transcoder.setBytesPerLine(0);
		WritableContainer<ByteBuffer> writable = TranscoderUtils.wrapWritable(buffer, transcoder);
		writable = TranscoderUtils.wrapWritable(writable, new GZIPEncoder());
		serializer.serialize(key, IOUtils.toOutputStream(maxEntrySize > 0 ? IOUtils.limitWritable(writable, maxEntrySize) : writable, true));
		writable.flush();
		return new String(IOUtils.toBytes(buffer), "ASCII").replace('/', '-');
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected void serializeValue(String key, Object value, WritableContainer<ByteBuffer> writable) throws IOException {
		DataSerializer serializer = getValueSerializer();
		if (serializer == null) {
			throw new IllegalArgumentException("No serializer found for the value");
		}
		writable = TranscoderUtils.wrapWritable(writable, new GZIPEncoder());
		serializer.serialize(value, IOUtils.toOutputStream(maxEntrySize > 0 ? IOUtils.limitWritable(writable, maxEntrySize - key.length()) : writable, true));
		writable.close();
	}
	
	@SuppressWarnings({ "rawtypes" })
	protected Object deserializeKey(String key) throws IOException {
		DataSerializer serializer = getKeySerializer();
		if (serializer == null) {
			throw new IllegalArgumentException("No serializer found for the key");
		}
		ReadableContainer<ByteBuffer> readable = IOUtils.wrap(key.replace('-', '/').getBytes("ASCII"), true);
		readable = TranscoderUtils.wrapReadable(readable, new Base64Decoder());
		readable = TranscoderUtils.wrapReadable(readable, new GZIPDecoder());
		return serializer.deserialize(IOUtils.toInputStream(readable, true));
	}
	@SuppressWarnings({ "rawtypes" })
	protected Object deserializeValue(ReadableContainer<ByteBuffer> readable) throws IOException {
		DataSerializer serializer = getValueSerializer();
		if (serializer == null) {
			throw new IllegalArgumentException("No serializer found for the value");
		}
		readable = TranscoderUtils.wrapReadable(readable, new GZIPDecoder());
		return serializer.deserialize(IOUtils.toInputStream(readable, true));
	}

	public long getMaxEntrySize() {
		return maxEntrySize;
	}

	public long getMaxCacheSize() {
		return maxCacheSize;
	}

	public DataSerializer<?> getKeySerializer() {
		return keySerializer;
	}

	public void setKeySerializer(DataSerializer<?> keySerializer) {
		this.keySerializer = keySerializer;
	}

	public DataSerializer<?> getValueSerializer() {
		return valueSerializer;
	}

	public void setValueSerializer(DataSerializer<?> valueSerializer) {
		this.valueSerializer = valueSerializer;
	}

	@Override
	public long getMaxTotalSize() {
		return maxCacheSize;
	}

	@Override
	public Collection<CacheEntry> getEntries() {
		List<CacheEntry> entries = new ArrayList<CacheEntry>();
		for (Resource resource : container) {
			entries.add(new ResourceEntry(this, resource));
		}
		return entries;
	}

	@Override
	public CacheEntry getEntry(Object key) {
		try {
			Resource child = container.getChild(serializeKey(key) + "." + extension);
			if (child != null) {
				return new ResourceEntry(this, child);
			}
		}
		catch (IOException e) {
			// ignore
		}
		return null;
	}

	@Override
	public String hash(Object key) {
		CacheEntry entry = getEntry(key);
		// if this boolean is turned on, we don't want to hash the actual data. it may be large and either way incur I/O overhead
		// instead we hash the metadata (like last modified), assuming they will correctly change when the data is updated
		if (hashMetadata && entry != null) {
			Date lastModified = entry.getLastModified();
			if (lastModified != null) {
				long size = entry.getSize();
				try {
					MessageDigest instance = MessageDigest.getInstance("MD5");
					byte [] digest = instance.digest((lastModified.toString() + "-" + size).getBytes("UTF-8"));
					return IOUtils.toString(new HexReadableCharContainer(IOUtils.wrap(digest, true)));
				}
				catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}
		// TODO: not implemented yet
		return null;
	}

}
