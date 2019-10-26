package com.adobe.platform.core.identity.services.cosmosdb.client;

import com.azure.data.cosmos.FeedOptions;
import com.azure.data.cosmos.FeedResponse;
import com.azure.data.cosmos.PartitionKeyDefinition;
import com.azure.data.cosmos.internal.AsyncDocumentClient;
import com.azure.data.cosmos.internal.DocumentCollection;
import com.azure.data.cosmos.internal.PartitionKeyRange;
import com.azure.data.cosmos.internal.RequestOptions;
import com.azure.data.cosmos.internal.ResourceResponse;
import com.azure.data.cosmos.internal.routing.CollectionRoutingMap;
import com.azure.data.cosmos.internal.routing.IServerIdentity;
import com.azure.data.cosmos.internal.routing.InMemoryCollectionRoutingMap;
import com.azure.data.cosmos.internal.routing.PartitionKeyInternal;
import com.azure.data.cosmos.internal.routing.PartitionKeyInternalHelper;
import com.azure.data.cosmos.internal.routing.Range;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * Helper class to retrieve partition metadata for a given collection. This is used for grouping
 * IDs by partition in
 * batch queries
 */
class CosmosDbPartitionMetadata {
    Logger logger = LoggerFactory.getLogger(CosmosDbPartitionMetadata.class.getSimpleName());

    private String collectionLink;
    private CosmosDbConfig cosmosDbConfig;
    private CollectionRoutingMap collectionRoutingMap;
    private List<String> partitionKeyRangeIds;
    private PartitionKeyDefinition partitionKeyDefinition;

    public CosmosDbPartitionMetadata(AsyncDocumentClient client, String collectionName,
                                     CosmosDbConfig cosmosDbConfig) {
        this.cosmosDbConfig = cosmosDbConfig;
        this.collectionLink = cosmosDbConfig.getCollectionLink(collectionName);
        reloadMetadata(client);
    }

    //init
    public void reloadMetadata(AsyncDocumentClient client) {
        ResourceResponse<DocumentCollection> response =
            client.readCollection(this.collectionLink, new RequestOptions()).blockFirst();
        this.partitionKeyDefinition = response.getResource().getPartitionKey();
        this.collectionRoutingMap = this.getCollectionRoutingMap(client, this.collectionLink);
        this.partitionKeyRangeIds = this.getCollectionPartitionKeyRangeIds(this.collectionRoutingMap);
    }

    /**
     * Group IDs by partition-range-id
     *
     * @param ids - A list of IDs. Each ID identifies a document in the collection
     * @return - A Map each Keys is a PartitionRangeId and the value is a Set of IDs that fall into this partition range.
     */
    public Map<String, Set<String>> groupIdsByPartitionRangeId(List<String> ids) {

        Map<String, Set<String>> partitionIdMap = new HashMap<>();
        Iterator<String> iter = ids.iterator();

        while (iter.hasNext()) {
            String id = iter.next();

            PartitionKeyInternal partitionKeyValue = PartitionKeyInternal.fromObjectArray(Collections.singletonList(id).toArray(), true);
            String effectivePartitionKey = PartitionKeyInternalHelper.getEffectivePartitionKeyString(partitionKeyValue, partitionKeyDefinition);
            String partitionRangeId = collectionRoutingMap.getRangeByEffectivePartitionKey(effectivePartitionKey).id();

            partitionIdMap.putIfAbsent(partitionRangeId, new HashSet<>(ids.size() / partitionKeyRangeIds.size()));
            partitionIdMap.get(partitionRangeId).add(id);
        }

        return partitionIdMap;
    }

    private CollectionRoutingMap getCollectionRoutingMap(AsyncDocumentClient client,String collectionLink){
        FeedResponse<PartitionKeyRange> partitionKeyRanges=
            client.readPartitionKeyRanges(collectionLink, new FeedOptions()).blockFirst();

        List<ImmutablePair<PartitionKeyRange, IServerIdentity>> ranges = partitionKeyRanges
            .results().stream()
            .map(r -> new ImmutablePair<PartitionKeyRange, IServerIdentity>(r, new IServerIdentity() {}))
            .collect(Collectors.toList());

        try {
            return InMemoryCollectionRoutingMap.tryCreateCompleteRoutingMap(ranges, collectionLink);
        } catch (Exception ex){
            throw new RuntimeException("Cannot create complete routing map for collectionLink " + collectionLink, ex);
        }
    }

    private List<String> getCollectionPartitionKeyRangeIds(CollectionRoutingMap collectionRoutingMap) {
        Range<String> fullRange = new Range<>(PartitionKeyInternalHelper.MinimumInclusiveEffectivePartitionKey,
            PartitionKeyInternalHelper.MaximumExclusiveEffectivePartitionKey, true, false);

        return collectionRoutingMap.getOverlappingRanges(fullRange).stream()
                                   .map(PartitionKeyRange::id)
                                   .collect(Collectors.toList());
    }
}