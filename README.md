# Resource-based Cache

A few things to note:

- you must explicitly set serializers for the key and the value or override the serializeKey/Value and deserializeKey/Value methods
- the resources used must be at least timestamped & finite and preferably also access tracked (otherwise the pruning will be based on last modified)
- no additional state is kept by the resource cache which means persist resources are (by default) a persistent cache and clustered resources are by default clustered
- by default you can not deserialize a key because no type information is present (could at some point appropriate the extension for this). This means that without setting a serializer that somehow knows the context, refreshing won't work.