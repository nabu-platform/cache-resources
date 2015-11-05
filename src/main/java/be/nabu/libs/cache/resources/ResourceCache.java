package be.nabu.libs.cache.resources;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import be.nabu.libs.cache.api.Cache;
import be.nabu.libs.cache.api.CacheRefresher;
import be.nabu.libs.cache.api.DataSerializer;
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

public class ResourceCache implements Cache {

	private long maxEntrySize, accessTimeout;
	private ManageableContainer<?> container;
	private CacheRefresher refresher;
	private String extension = "bin";
	private long maxCacheSize;
	private long refreshTimeout;
	private DataSerializer<?> keySerializer, valueSerializer;
	
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
		put(serializedKey, value);
		return true;
	}

	private synchronized void put(String serializedKey, Object value) throws IOException {
		Resource child = container.getChild(serializedKey + "." + extension);
		if (child == null) {
			child = container.create(serializedKey + "." + extension, "application/octet-stream");
		}
		serializeValue(value, new ResourceWritableContainer((WritableResource) child));
		if (getSize() > maxCacheSize) {
			prune();
		}
	}

	public List<Resource> getEntries() {
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
				// if it was last modified before the refresh timeout, refresh it
				if (resource instanceof TimestampedResource && ((TimestampedResource) resource).getLastModified().before(new Date(new Date().getTime() - refreshTimeout))) {
					// strip the extension again
					String keyValue = resource.getName().replaceAll("\\..*$", "");
					Object deserializedKey = deserializeKey(keyValue);
					// it can be null if there is not enough metadata to deserialize the key, in this case it is skipped for refresh
					if (deserializedKey != null) {
						Object refreshed = refresher.refresh(deserializedKey);
						if (refreshed != null) {
							put(keyValue, refreshed);
						}
					}
				}
			}
		}
	}
	
	@Override
	public synchronized void prune() throws IOException {
		List<Resource> list = getEntries();
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
		while(!list.isEmpty()) {
			Resource entryToDelete = list.remove(0);
			Date dateToUse = entryToDelete instanceof AccessTrackingResource 
				? ((AccessTrackingResource) entryToDelete).getLastAccessed()
				: ((TimestampedResource) entryToDelete).getLastModified();
			
			// if we have gone over size or the entry is timed out, delete it
			// because we have ordered by last accessed ascending, once we hit the first non-timed out we should stop (if the size constraint is not met)
			long totalSize = getSize();
			if (totalSize > maxCacheSize || dateToUse.before(new Date(new Date().getTime() - accessTimeout))) {
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

	@Override
	public Object get(Object key) throws IOException {
		String serializedKey = serializeKey(key);
		Resource child = container.getChild(serializedKey + "." + extension);
		if (child == null) {
			// it could be that the entry does not exist or that it is still being added (e.g. the resource exists but not yet the value mapping)
			// it is however a cache so best effort
			return null;
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
	public long getSize() {
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
		serializer.serialize(key, IOUtils.toOutputStream(writable));
		writable.flush();
		return new String(IOUtils.toBytes(buffer), "ASCII").replace('/', '-');
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected void serializeValue(Object value, WritableContainer<ByteBuffer> writable) throws IOException {
		DataSerializer serializer = getValueSerializer();
		if (serializer == null) {
			throw new IllegalArgumentException("No serializer found for the value");
		}
		writable = TranscoderUtils.wrapWritable(writable, new GZIPEncoder());
		serializer.serialize(value, IOUtils.toOutputStream(IOUtils.limitWritable(writable, maxEntrySize)));
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

	public long getAccessTimeout() {
		return accessTimeout;
	}

	public long getMaxCacheSize() {
		return maxCacheSize;
	}

	public long getRefreshTimeout() {
		return refreshTimeout;
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
	
}
