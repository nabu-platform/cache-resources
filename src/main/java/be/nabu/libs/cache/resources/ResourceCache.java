package be.nabu.libs.cache.resources;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import be.nabu.libs.cache.DataSerializationFactory;
import be.nabu.libs.cache.api.Cache;
import be.nabu.libs.cache.api.CacheRefresher;
import be.nabu.libs.cache.api.DataSerializer;
import be.nabu.libs.resources.ResourceReadableContainer;
import be.nabu.libs.resources.ResourceWritableContainer;
import be.nabu.libs.resources.api.FiniteResource;
import be.nabu.libs.resources.api.ManageableContainer;
import be.nabu.libs.resources.api.ReadableResource;
import be.nabu.libs.resources.api.Resource;
import be.nabu.libs.resources.api.ResourceContainer;
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

public class ResourceCache implements Cache {

	private long maxEntrySize, accessTimeout;
	private ManageableContainer<?> container;
	private CacheRefresher refresher;
	private String extension = "bin";
	private Map<String, ResourceCacheEntry> entries = new HashMap<String, ResourceCacheEntry>();
	private long totalSize;
	private long maxCacheSize;
	private long refreshTimeout;
	
	public ResourceCache(ManageableContainer<?> container, long maxEntrySize, long maxCacheSize, long accessTimeout, CacheRefresher cacheRefresher, long refreshTimeout) {
		this.container = container;
		this.maxEntrySize = maxEntrySize;
		this.maxCacheSize = maxCacheSize;
		this.accessTimeout = accessTimeout;
		this.refresher = cacheRefresher;
		this.refreshTimeout = refreshTimeout;
	}
	
	@Override
	public synchronized boolean put(Object key, Object value) throws IOException {
		String serializedKey = serializeKey(key);
		put(serializedKey, key.getClass(), value);
		return true;
	}

	private void put(String serializedKey, Class<?> keyClass, Object value) throws IOException {
		long sizeToAdd = serializedKey.length();
		Resource child = container.getChild(serializedKey + "." + extension);
		if (child == null) {
			child = container.create(serializedKey + "." + extension, "application/octet-stream");
		}
		entries.put(serializedKey, new ResourceCacheEntry(serializedKey, keyClass, value.getClass()));
		serializeValue(value, new ResourceWritableContainer((WritableResource) child));
		sizeToAdd += ((FiniteResource) child).getSize();
		totalSize += sizeToAdd;
		if (totalSize > maxCacheSize) {
			prune();
		}
	}

	@Override
	public synchronized void refresh() throws IOException {
		if (refresher != null) {
			List<ResourceCacheEntry> list = new ArrayList<ResourceCacheEntry>(entries.values());
			for (ResourceCacheEntry entry : list) {
				// if it was created before the refresh timeout, refresh it
				if (entry.getCreated().before(new Date(new Date().getTime() - refreshTimeout))) {
					Object deserializedKey = deserializeKey(entry.getKey(), entry.getKeyClass());
					Object refreshed = refresher.refresh(deserializedKey);
					if (refreshed != null) {
						clear(entry.getKey(), true);
						put(entry.getKey(), entry.getKeyClass(), refreshed);
					}
				}
			}
		}
	}
	
	@Override
	public synchronized void prune() throws IOException {
		List<ResourceCacheEntry> list = new ArrayList<ResourceCacheEntry>(entries.values());
		// order by last accessed date, oldest is first
		Collections.sort(list);
		while(!list.isEmpty()) {
			ResourceCacheEntry entryToDelete = list.remove(0);
			// if we have gone over size or the entry is timed out, delete it
			// because we have ordered by last accessed ascending, once we hit the first non-timed out we should stop (if the size constraint is not met)
			if (totalSize > maxCacheSize || entryToDelete.getLastAccessed().before(new Date(new Date().getTime() - accessTimeout))) {
				long sizeToRemove = entryToDelete.getKey().length();
				Resource child = container.getChild(entryToDelete.getKey() + "." + extension);
				if (child != null) {
					sizeToRemove += ((FiniteResource) child).getSize();
					container.delete(entryToDelete.getKey() + "." + extension);
				}
				entries.remove(entryToDelete.getKey());
				totalSize -= sizeToRemove;
			}
			else {
				break;
			}
		}
	}

	@Override
	public Object get(Object key) throws IOException {
		String serializedKey = serializeKey(key);
		ResourceCacheEntry resourceCacheEntry = entries.get(serializedKey);
		if (resourceCacheEntry == null) {
			// it could be that the entry does not exist or that it is still being added (e.g. the resource exists but not yet the value mapping)
			// it is however a cache so best effort
			return null;
		}
		Resource child = container.getChild(serializedKey + "." + extension);
		if (child == null) {
			// same reasoning
			return null;
		}
		resourceCacheEntry.accessed();
		return child == null ? null : deserializeValue(new ResourceReadableContainer((ReadableResource) child), resourceCacheEntry.getValueClass());
	}

	@Override
	public void clear(Object key) throws IOException {
		String serializedKey = serializeKey(key);
		clear(serializedKey, false);
	}

	private synchronized void clear(String serializedKey, boolean forRefresh) throws IOException {
		long sizeToRemove = serializedKey.length();
		Resource child = container.getChild(serializedKey + "." + extension);
		if (child != null) {
			sizeToRemove += ((FiniteResource) child).getSize();
			// if we are clearing for refresh, don't delete the resource
			if (!forRefresh) {
				container.delete(serializedKey + "." + extension);
			}
		}
		entries.remove(serializedKey);
		totalSize -= sizeToRemove;
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
		entries.clear();
		totalSize = 0;
	}

	@Override
	public long getSize() {
		return totalSize;
	}

	protected DataSerializer<?> getKeySerializer(Class<?> keyClass) {
		return DataSerializationFactory.getInstance().getSerializer(keyClass);
	}
	
	protected DataSerializer<?> getValueSerializer(Class<?> valueClass) {
		return DataSerializationFactory.getInstance().getSerializer(valueClass);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	String serializeKey(Object key) throws IOException {
		DataSerializer serializer = getKeySerializer(key.getClass());
		if (serializer == null) {
			throw new IllegalArgumentException("Can not store an object of type '" + key.getClass().getName() + "' in the cache as it can not be serialized");
		}
		ByteBuffer buffer = IOUtils.newByteBuffer();
		Base64Encoder transcoder = new Base64Encoder();
		transcoder.setBytesPerLine(0);
		WritableContainer<ByteBuffer> writable = TranscoderUtils.wrapWritable(buffer, transcoder);
		writable = TranscoderUtils.wrapWritable(writable, new GZIPEncoder());
		serializer.serialize(key, IOUtils.toOutputStream(writable));
		writable.flush();
		return new String(IOUtils.toBytes(buffer), "ASCII").replace('/', '-');
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	void serializeValue(Object value, WritableContainer<ByteBuffer> writable) throws IOException {
		DataSerializer serializer = getValueSerializer(value.getClass());
		if (serializer == null) {
			throw new IllegalArgumentException("Can not store an object of type '" + value.getClass().getName() + "' in the cache as it can not be serialized");
		}
		writable = TranscoderUtils.wrapWritable(writable, new GZIPEncoder());
		serializer.serialize(value, IOUtils.toOutputStream(IOUtils.limitWritable(writable, maxEntrySize)));
		writable.close();
	}
	
	@SuppressWarnings({ "rawtypes" })
	Object deserializeKey(String key, Class<?> keyClass) throws IOException {
		DataSerializer serializer = getKeySerializer(keyClass);
		if (serializer == null) {
			throw new IllegalArgumentException("Can not retrieve an object of type '" + keyClass.getName() + "' in the cache as it can not be serialized");
		}
		ReadableContainer<ByteBuffer> readable = IOUtils.wrap(key.replace('-', '/').getBytes("ASCII"), true);
		readable = TranscoderUtils.wrapReadable(readable, new Base64Decoder());
		readable = TranscoderUtils.wrapReadable(readable, new GZIPDecoder());
		return serializer.deserialize(IOUtils.toInputStream(readable, true));
	}
	@SuppressWarnings({ "rawtypes" })
	Object deserializeValue(ReadableContainer<ByteBuffer> readable, Class<?> valueClass) throws IOException {
		DataSerializer serializer = getValueSerializer(valueClass);
		if (serializer == null) {
			throw new IllegalArgumentException("Can not retrieve an object of type '" + valueClass.getName() + "' in the cache as it can not be serialized");
		}
		readable = TranscoderUtils.wrapReadable(readable, new GZIPDecoder());
		return serializer.deserialize(IOUtils.toInputStream(readable, true));
	}

	public long getMaxEntrySize() {
		return maxEntrySize;
	}

	public long getAccessTimeout() {
		return accessTimeout;
	}

	public long getMaxCacheSize() {
		return maxCacheSize;
	}

	public long getRefreshTimeout() {
		return refreshTimeout;
	}
	
	public int getAmountOfEntries() {
		return entries.size();
	}
}
