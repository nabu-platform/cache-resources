package be.nabu.libs.cache.resources;

import java.io.IOException;
import java.net.URI;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;

import be.nabu.libs.cache.api.Cache;
import be.nabu.libs.cache.api.CacheProvider;
import be.nabu.libs.cache.api.CacheRefresher;
import be.nabu.libs.cache.impl.LastModifiedTimeoutManager;
import be.nabu.libs.resources.ResourceUtils;
import be.nabu.libs.resources.api.ManageableContainer;
import be.nabu.libs.resources.api.Resource;

public class ResourceCacheProvider implements CacheProvider {

	private ManageableContainer<?> root;
	private Map<String, Cache> caches = new HashMap<String, Cache>();
	private long maxEntrySize;
	private long maxTotalSize;
	private long cacheTimeout;
	private CacheRefresher refresher;
	
	public ResourceCacheProvider(ManageableContainer<?> root, long maxEntrySize, long maxTotalSize, long cacheTimeout, CacheRefresher refresher) throws IOException {
		this.maxEntrySize = maxEntrySize;
		this.maxTotalSize = maxTotalSize;
		this.cacheTimeout = cacheTimeout;
		this.refresher = refresher;
		this.root = root;
	}
	
	public ResourceCacheProvider(URI root, Principal principal, long maxEntrySize, long maxTotalSize, long accessTimeout, CacheRefresher refresher, long refreshTimeout) throws IOException {
		this((ManageableContainer<?>) ResourceUtils.mkdir(root, principal), maxEntrySize, maxTotalSize, accessTimeout, refresher);
	}
	
	@Override
	public Cache get(String name) throws IOException {
		if (!caches.containsKey(name)) {
			synchronized(caches) {
				if (!caches.containsKey(name)) {
					Resource child = root.getChild(name);
					if (child == null) {
						child = root.create(name, Resource.CONTENT_TYPE_DIRECTORY);
					}
					caches.put(name, new ResourceCache((ManageableContainer<?>) child, maxEntrySize, maxTotalSize, refresher, new LastModifiedTimeoutManager(cacheTimeout)));
				}
			}
		}
		return null;
	}

	@Override
	public void remove(String name) throws IOException {
		if (caches.containsKey(name)) {
			synchronized(caches) {
				caches.remove(name);
			}
		}
	}

}
