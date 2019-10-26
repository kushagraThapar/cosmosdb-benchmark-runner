package com.adobe.platform.core.identity.services.cosmosdb.client;

import com.adobe.platform.core.identity.services.cosmosdb.util.CosmosDbException;
import com.azure.data.cosmos.ConnectionMode;
import com.azure.data.cosmos.ConnectionPolicy;
import com.azure.data.cosmos.ConsistencyLevel;
import com.azure.data.cosmos.CosmosBridgeInternal;
import com.azure.data.cosmos.CosmosClient;
import com.azure.data.cosmos.CosmosClientBuilder;
import com.azure.data.cosmos.FeedOptions;
import com.azure.data.cosmos.FeedResponse;
import com.azure.data.cosmos.IndexingMode;
import com.azure.data.cosmos.IndexingPolicy;
import com.azure.data.cosmos.PartitionKey;
import com.azure.data.cosmos.PartitionKeyDefinition;
import com.azure.data.cosmos.RequestTimeoutException;
import com.azure.data.cosmos.SqlParameter;
import com.azure.data.cosmos.SqlParameterList;
import com.azure.data.cosmos.SqlQuerySpec;
import com.azure.data.cosmos.internal.AsyncDocumentClient;
import com.azure.data.cosmos.internal.Database;
import com.azure.data.cosmos.internal.Document;
import com.azure.data.cosmos.internal.DocumentCollection;
import com.azure.data.cosmos.internal.HttpConstants;
import com.azure.data.cosmos.internal.RequestOptions;
import com.azure.data.cosmos.internal.ResourceResponse;
import com.azure.data.cosmos.internal.Utils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class V3AsyncCosmosDbClient implements CosmosDbClient {

    private static final Logger logger =
        LoggerFactory.getLogger(V3AsyncCosmosDbClient.class.getSimpleName());

    //using for an experiment, todo: remove
    private final ExecutorService executor;

    private final CosmosDbConfig cfg;
    private final AsyncDocumentClient asyncDocumentClient;
    private final Map<String, CosmosDbPartitionMetadata> partitionMetadataMap =
        new ConcurrentHashMap<>();

    public V3AsyncCosmosDbClient(CosmosDbConfig cfg) {
        this.cfg = cfg;
        CosmosClient client = createCosmosClient(cfg.serviceEndpoint, cfg.masterKey,
            cfg.connectionMode, cfg.consistencyLevel,
            cfg.maxPoolSize, cfg.requestTimeoutInMillis);
        this.asyncDocumentClient = CosmosBridgeInternal.getAsyncDocumentClient(client);
        this.executor = Executors.newFixedThreadPool(1000);
        //Executors.newCachedThreadPool(
        //new ThreadFactoryBuilder().setNameFormat("AsyncCosmosDbClient-%d").build());
    }

    // -------------  WIP

    private synchronized void loadPartitionMetadataIfNeeded(String collectionName) {
        // check and update partition metadata map for collection
        if (!partitionMetadataMap.containsKey(collectionName)) {
            logger.info("Fetching Partition Metadata for collection {} ...", collectionName);
            partitionMetadataMap.put(collectionName,
                new CosmosDbPartitionMetadata(asyncDocumentClient, collectionName, cfg));
            logger.info("Fetching Partition Metadata for collection {} complete.", collectionName);
        }
    }

    @Override
    public SimpleResponse readDocuments(String collectionName, List<String> docIdList,
                                        int batchQueryMaxSize)
        throws CosmosDbException {
        return makeSync(
            readDocumentBatch(collectionName, docIdList, batchQueryMaxSize)
                .flatMap(Flux::fromIterable)
                .map(f -> {
                    List<SimpleDocument> docs = f.results().stream()
                                                 .map(a -> new SimpleDocument(a.id(), a.getMap())).collect(Collectors.toList());

                    int statusCode = 200; //fixme

                    SimpleResponse simpleResponse = new SimpleResponse(docs, statusCode,
                        f.requestCharge(), 0, f.activityId());
                    simpleResponse.setFeedResponseDiagnostics(f.feedResponseDiagnostics());
                    return simpleResponse;

                }).collectList()
                .map(respList -> {
                    List<SimpleDocument> newDocs = new ArrayList<>();
                    SimpleResponse newResp = new SimpleResponse(newDocs, 500, 0, 0, "");
                    respList.forEach(resp -> {
                        newResp.getDocuments().addAll(resp.documents);
                        newResp.ruUsed += resp.ruUsed;
                        newResp.statusCode = resp.statusCode; //fixme
                        newResp.activityId = newResp.activityId + ", " + resp.activityId;
                    });
                    return newResp;
                }).flux()
        );
    }

    Flux<List<FeedResponse<Document>>> readDocumentBatch(String collectionName, List<String> docIds,
                                                         int batchQueryMaxSize) {
        // check and update partition metadata map for collection
        if (!partitionMetadataMap.containsKey(collectionName)) {
            loadPartitionMetadataIfNeeded(collectionName);
        }

        CosmosDbPartitionMetadata partitionMeta = partitionMetadataMap.get(collectionName);
        //todo change to name
        Map<String, Set<String>> groupedIds = partitionMeta.groupIdsByPartitionRangeId(docIds);


        List<Flux<FeedResponse<Document>>> obsList = groupedIds.entrySet().stream().flatMap(e -> {
            //Break up keys in each partition into batchQueryMaxSize
            List<String> collection = new ArrayList<>(e.getValue());
            List<List<String>> lists = new ArrayList<>();

            for (int i = 0; i < collection.size(); i += batchQueryMaxSize) {
                int end = Math.min(collection.size(), i + batchQueryMaxSize);
                lists.add(collection.subList(i, end));
            }

            return lists.stream().map(l -> Pair.of(e.getKey(), l));
        })
                                                               .map(subQuery -> {
                                                                   //Issue a queryDocument per
                                                                   // item emitted by stream
                                                                   String partitionId =
                                                                       subQuery.getLeft();
                                                                   List<String> queryIds =
                                                                       subQuery.getRight();

                                                                   String sqlQuery =
                                                                       String.format(CosmosConstants.QUERY_STRING_BATCH, CosmosConstants.COSMOS_DEFAULT_COLUMN_KEY, queryIds.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(",")));
                                                                   return asyncDocumentClient.queryDocuments(cfg.getCollectionLink(collectionName), sqlQuery, generateFeedOptions(partitionId)).collectList();
                                                               }).map(t -> t.flatMapIterable(p -> p)).collect(Collectors.toList());

        return Flux.fromIterable(obsList)
                   .flatMap(task -> task) //todo :: check
                   .collectList().flux();
    }


    /**
     * For Experiment, to be removed
     *
     * @param collectionName
     * @param docIds
     * @param batchQueryMaxSize
     * @return
     */
    public SimpleResponse readDocumentsMultiThread(String collectionName, List<String> docIds,
                                                   int batchQueryMaxSize) {
        // check and update partition metadata map for collection
        if (!partitionMetadataMap.containsKey(collectionName)) {
            loadPartitionMetadataIfNeeded(collectionName);
        }

        CosmosDbPartitionMetadata partitionMeta = partitionMetadataMap.get(collectionName);
        //todo change to name
        Map<String, Set<String>> groupedIds = partitionMeta.groupIdsByPartitionRangeId(docIds);

        //create tasks
        List<Callable<List<FeedResponse<Document>>>> callables =
            groupedIds.entrySet().stream().flatMap(e -> {
            //Break up keys in each partition into batchQueryMaxSize
            List<String> collection = new ArrayList<>(e.getValue());
            List<List<String>> lists = new ArrayList<>();

            for (int i = 0; i < collection.size(); i += batchQueryMaxSize) {
                int end = Math.min(collection.size(), i + batchQueryMaxSize);
                lists.add(collection.subList(i, end));
            }

            return lists.stream().map(l -> Pair.of(e.getKey(), l));
        })
                                                                           .map(subQuery -> {
                                                                               //Issue a
                                                                               // queryDocument
                                                                               // per item
                                                                               // emitted by stream
                                                                               String partitionId = subQuery.getLeft();
                                                                               List<String> queryIds = subQuery.getRight();

                                                                               String sqlQuery =
                                                                                   String.format(CosmosConstants.QUERY_STRING_BATCH, CosmosConstants.COSMOS_DEFAULT_COLUMN_KEY, queryIds.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(",")));
                                                                               Callable<List<FeedResponse<Document>>> task = () -> asyncDocumentClient.queryDocuments(cfg.getCollectionLink(collectionName), sqlQuery, generateFeedOptions(partitionId)).collectList().block();
                                                                               return task;
                                                                           }).collect(Collectors.toList());


        List<SimpleDocument> newDocs = new ArrayList<>();
        SimpleResponse newResp = new SimpleResponse(newDocs, 500, 0, 0, "");

        //launch all tasks
        try {
            executor.invokeAll(callables)
                    .stream()
                    .map(future -> {
                        try {
                            return future.get();
                        } catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                    })
                    .flatMap(f -> f.stream().map(r -> new SimpleResponse(
                        r.results().stream().map(V3AsyncCosmosDbClient::toSimpleDocument).collect(Collectors.toList()),
                        200,
                        r.requestCharge(),
                        0,
                        r.activityId())))
                    .forEach(resp -> {
                        newResp.getDocuments().addAll(resp.documents);
                        newResp.ruUsed += resp.ruUsed;
                        newResp.statusCode = resp.statusCode; //fixme
                        newResp.activityId = newResp.activityId + ", " + resp.activityId;
                    });
            return newResp;
        } catch (InterruptedException e) {
            throw new RuntimeException("Thread interrupted", e);
        }
    }


    // -------------  Implementation of the Client Interface

    @Override
    public CosmosDbConfig getConfig() {
        return this.cfg;
    }

    public Flux<ResourceResponse<Document>> getDocument(String collectionName, String docId) {
        final AtomicBoolean retried = new AtomicBoolean();
        return asyncDocumentClient.readDocument(cfg.getDocumentLink(collectionName, docId),
            getRequestOptions(docId))
                                  .retryWhen(errors -> errors.flatMap(error -> {
                                      if (error instanceof RequestTimeoutException) {
                                          if (retried.compareAndSet(true, true)) {
                                              logger.error("retried document read failed: ", error);
                                              return Flux.error(error);
                                          }
                                          RequestTimeoutException timeoutException =
                                              (RequestTimeoutException)error;
                                          long milliseconds =
                                              timeoutException.retryAfterInMilliseconds();
                                          return Flux.empty().delayElements(Duration.ofMillis(milliseconds));
                                      } else {
                                          logger.error("document read failed: ", error);
                                      }
                                      return Flux.error(error); // accept any error other than a
                                      // request timeout
                                  }));
    }

    @Override
    public SimpleResponse readDocument(String collectionName, String docId) throws CosmosDbException {
        return makeSync(
            getDocument(collectionName, docId)
                .map(r -> new SimpleResponse(new SimpleDocument(r.getResource().id(),
                    r.getResource().getMap()), r.getStatusCode(), r.getRequestCharge(),
                    0,//r.getRequestLatency().toMillis(),
                    r.getActivityId())));
    }

    public Flux<ResourceResponse<Document>> createDocument(String collectionName, Document doc) {
        RequestOptions options = getRequestOptions(doc.id());
        return asyncDocumentClient.createDocument(cfg.getCollectionLink(collectionName), doc,
            options, false);
    }

    @Override
    public SimpleResponse createDocument(String collectionName, SimpleDocument sDoc) throws CosmosDbException {
        Map<String, Object> properties = sDoc.properties;
        Document doc = null;
        try {
            doc = new Document(Utils.getSimpleObjectMapper().writeValueAsString(properties));
            doc.id(sDoc.id);
        } catch (JsonProcessingException e) {
            logger.error("Error occurred while writing map as string value", e);
        }
        //        sDoc.properties.entrySet().stream().forEach(a -> doc.set(a.getKey(), a.getValue
        //        ()));

        return makeSync(
            createDocument(collectionName, doc)
                .map(r -> new SimpleResponse(new SimpleDocument(r.getResource().id(),
                    r.getResource().getMap()), r.getStatusCode(), r.getRequestCharge(),
                    0,//r.getRequestLatency().toMillis(),
                    r.getActivityId())));
    }

    @Override
    public void close() {
        asyncDocumentClient.close();
        executor.shutdownNow();
    }

    // -------------  Instance Helpers

    /**
     * @return For each partition in collection, returns a list of Document IDs.
     */
    public Flux<List<List<String>>> getIdsPerPartition(String collectionName,
                                                       int itemsPerPartition) {
        return getIdsPerPartition(cfg.dbName, collectionName, itemsPerPartition);
    }

    public Flux<List<List<String>>> getIdsPerPartition(String dbName, String collectionName,
                                                       int itemsPerPartition) {
        String collectionLink = cfg.getCollectionLink(dbName, collectionName);

        //todo :: errorHandling

        Flux<List<List<String>>> keysByPartitionObs = asyncDocumentClient
            .readPartitionKeyRanges(collectionLink, generateFeedOptions(null))
            .retry()
            .map(FeedResponse::results)
            .flatMapIterable(item -> item)
            .flatMap(pkRange ->
                asyncDocumentClient
                    .queryDocuments(collectionLink, generateTopNQuery(itemsPerPartition),
                        generateFeedOptions(pkRange.id())))
            .map(a -> a.results().stream().map(Document::id).collect(Collectors.toList()))
            .collectList().flux();

        return keysByPartitionObs;
    }

    public Flux<Database> getDatabase() {
        return getDatabase(cfg.dbName);
    }

    public Flux<Database> getDatabase(String dbName) {
        Flux<Database> dbObs = asyncDocumentClient
            .queryDatabases(new SqlQuerySpec(CosmosConstants.ROOT_QUERY,
                new SqlParameterList(new SqlParameter("@id", dbName))), null)
            .flatMap(feedResponse -> {
                if (feedResponse.results().isEmpty()) {
                    return Flux.error(new RuntimeException("cannot find database " + dbName));
                } else {
                    return Flux.just(feedResponse.results().get(0));
                }
            });

        return dbObs;
    }

    public Flux<DocumentCollection> getCollection(String collectionName) {
        return getCollection(cfg.dbName, collectionName);
    }

    public Flux<DocumentCollection> getCollection(String dbName, String collectionName) {
        //todo :: errorHandling
        Flux<DocumentCollection> docCollObs = asyncDocumentClient
            .queryCollections(cfg.getDatabaseLink(dbName),
                new SqlQuerySpec(CosmosConstants.ROOT_QUERY,
                    new SqlParameterList(new SqlParameter("@id", collectionName))), null)
            .flatMap(feedResponse -> {
                if (feedResponse.results().isEmpty()) {
                    return Flux.error(new CosmosDbException("Cannot find collection "
                        + collectionName + "in db " + dbName + " !", null));
                } else {
                    return Flux.just(feedResponse.results().get(0));
                }
            });

        return docCollObs;
    }

    public Flux<Boolean> createCollection(String collectionName, String partitionKey,
                                          int createWithRu, int postCreateRu,
                                          String consistencyLevel) {
        return createCollection(cfg.dbName, collectionName, partitionKey, createWithRu,
            postCreateRu, consistencyLevel);
    }

    public Flux<Boolean> createCollection(String dbName, String collectionName, String partitionKey,
                                          int createWithRu, int postCreateRu,
                                          String consistencyLevel) {
        String databaseLink = cfg.getDatabaseLink(dbName);

        // Set Partition Definition
        PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
        partitionKeyDefinition.paths(Arrays.asList("/" + partitionKey));

        // Set DocumentCollection Properties
        DocumentCollection documentCollection = new DocumentCollection();
        documentCollection.id(collectionName);
        documentCollection.setPartitionKey(partitionKeyDefinition);
        documentCollection.setIndexingPolicy(getDefaultIndexingPolicy());
        //todo :: set indexing policy to exclude paths - needed for write benchmarks

        // Set RU limits to createWithRu. Note this controls the partition count.
        RequestOptions requestOptions = new RequestOptions();
        requestOptions.setOfferThroughput(createWithRu);
        requestOptions.setConsistencyLevel(ConsistencyLevel.valueOf(consistencyLevel));

        // Build the request
        Flux<Boolean> createStatus = asyncDocumentClient
            .createCollection(databaseLink, documentCollection, requestOptions)
            .switchMap(response -> {
                // Set the RU limits to postCreateRu.
                if (response == null && response.getResource() == null && response.getResource().resourceId() == null) {
                    return Flux.error(new RuntimeException("Failed creating collection with name=" +
                        collectionName + "databaseLink=" + databaseLink + ". Response from " +
                        "createCollection was null"));
                }
                logger.info("Successfully created collection={} databaseLink={}.", collectionName
                    , databaseLink);

                if (postCreateRu == createWithRu) {
                    return Flux.just(true);
                }

                logger.info("Attempting to set RU post collection creation from {} to {} ...",
                    createWithRu,
                    postCreateRu);

                return asyncDocumentClient.queryOffers(String.format(CosmosConstants.OFFER_QUERY_STR_FMT,
                    response.getResource().resourceId()), null)
                                          .switchMap(page -> Flux.fromIterable(page.results()))
                                          .take(1)
                                          .switchMap(offer -> {
                                              offer.setThroughput(postCreateRu);
                                              return asyncDocumentClient.replaceOffer(offer);
                                          })
                                          .map(newOffer -> {
                                              logger.info("Successfully changed RU from {} to {} " +
                                                      "for collection={} databaseLink={}.",
                                                  createWithRu, postCreateRu, collectionName,
                                                  databaseLink);
                                              if (newOffer.getResource().getThroughput() != postCreateRu) {
                                                  Flux.error(new RuntimeException("Failed to " +
                                                      "update RU Offer from {} to {} for " +
                                                      "collection=" + collectionName + " " +
                                                      "databaseLink= + " + databaseLink + "."));
                                              }
                                              return true;
                                          })
                                          .onErrorResume(e ->
                                              Flux.error(new RuntimeException("Failed to update " +
                                                  "RU offer from " + createWithRu +
                                                  " to " + postCreateRu + " for collection " +
                                                  "with name=" + collectionName + " databaseLink" +
                                                  "=" + databaseLink +
                                                  ".", e)));
            })
            .onErrorResume(e ->
                Flux.error(new RuntimeException("Failed to create collection with name=name=" +
                    collectionName + " databaseLink=" + databaseLink + ".", e)));

        // Add deferred logging statement to the observable
        //logger.info("Creating collection with name={} databaseLink={} ...", collectionName,
        // databaseLink);
        Flux<Boolean> logObs = Flux.defer(() -> {
            logger.info("Creating collection with name={} databaseLink={} ...", collectionName,
                databaseLink);
            return Flux.just(true);
        });

        return logObs.mergeWith(createStatus);
    }

    public Flux<Long> getCollectionSize(String collectionName) {
        FeedOptions options = new FeedOptions();
        options.enableCrossPartitionQuery(true);

        //todo :: errorHandling
        return asyncDocumentClient
            .queryDocuments(cfg.getCollectionLink(collectionName), CosmosConstants.COUNT_QUERY,
                options)
            .flatMap(feedResponse -> {
                if (feedResponse.results().isEmpty()) {
                    return Flux.just(0L);
                } else {
                    return Flux.just(feedResponse.results().get(0).getLong(CosmosConstants.AGGREGATE_PROPERTY));
                }
            });
    }

    public Flux<Boolean> deleteCollection(String collectionName) {
        Flux<Boolean> deleteStatus = asyncDocumentClient
            .deleteCollection(cfg.getCollectionLink(collectionName), new RequestOptions())
            .map(response -> {
                logger.info("Deleted collection {}.", collectionName);
                return true;
            })
            .onErrorResume(e -> {
                if (e instanceof DocumentClientException) {
                    DocumentClientException dce = (DocumentClientException)e;
                    if (dce.getStatusCode() == HttpConstants.StatusCodes.NOTFOUND) {
                        logger.warn("Collection `{}` doesn't exists. Delete operation skipped !",
                            collectionName);
                        return Flux.just(true);
                    }
                } else {
                    logger.error("Unable to delete collection {}. Delete operation failed !",
                        collectionName);
                }
                return Flux.error(e);
            });

        return deleteStatus;
    }

    public AsyncDocumentClient getDocumentClient() {
        return this.asyncDocumentClient;
    }

    private RequestOptions getRequestOptions() {
        return getRequestOptions(null);
    }

    private RequestOptions getRequestOptions(String pKeyStr) {
        RequestOptions options = new RequestOptions();
        options.setConsistencyLevel(ConsistencyLevel.valueOf(cfg.consistencyLevel));
        if (pKeyStr != null) {
            PartitionKey partitionKey = new PartitionKey(pKeyStr);
            options.setPartitionKey(partitionKey);
        }
        return options;
    }


    // -------------  Static Helpers
    private static CosmosClient createCosmosClient(String serviceEndpoint, String masterKey,
                                                   String connectionMode, String consistencyLevel,
                                                   int maxPoolSize, int requestTimeoutInMillis) {

        com.azure.data.cosmos.ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.requestTimeoutInMillis(requestTimeoutInMillis);

        connectionPolicy.connectionMode(ConnectionMode.valueOf(connectionMode.toUpperCase()));
        connectionPolicy.maxPoolSize(maxPoolSize);

        return new CosmosClientBuilder()
            .endpoint(serviceEndpoint)
            .key(masterKey)
            .connectionPolicy(connectionPolicy)
            .consistencyLevel(ConsistencyLevel.valueOf(consistencyLevel.toUpperCase()))
            .build();
    }

    private static IndexingPolicy getDefaultIndexingPolicy() {
        IndexingPolicy indexingPolicy = new IndexingPolicy();
        indexingPolicy.automatic(false);
        indexingPolicy.indexingMode(IndexingMode.NONE);

        return indexingPolicy;
    }

    private static FeedOptions generateFeedOptions(String partitionKey) {
        FeedOptions feedOptions = new FeedOptions();
        if (partitionKey != null) {
            feedOptions.partitionKey(new PartitionKey(partitionKey));
        }
        feedOptions.maxItemCount(10000);
        feedOptions.maxBufferedItemCount(10000);
        return feedOptions;
    }

    private static String generateTopNQuery(int limit) {
        return "SELECT TOP " + limit + " * FROM c";
    }


    // -------------  Obs

    // -------------  Obs
    public static <T> T makeSync(Flux<T> flux) throws CosmosDbException {
        try {
            return flux.blockFirst();
        } catch (Throwable th) {
            CosmosDbException ex = new CosmosDbException("A cosmosDB exception has occurred!", th.getCause(), false, true);
            ex.setStackTrace(th.getStackTrace());
            throw ex;
        }
    }

    // --------------- Helpers
    //    public static Document toDocument(SimpleDocument sDoc){
    //        Document doc = new Document();
    //        doc.id(sDoc.id);
    //        sDoc.properties.entrySet().stream().forEach(entry -> doc.set(entry.getKey(), entry.getValue()));
    //        return doc;
    //    }

    public static SimpleDocument toSimpleDocument(Document doc) {
        return new SimpleDocument(doc.id(), doc.getMap());
    }

    public void verifyCollectionsExist(List<String> collectionNames) {
        logger.info("Verifying that Database/Collections exists ...");
        Flux.fromIterable(collectionNames)
            .flatMap(this::getCollection)
            .subscribeOn(Schedulers.parallel())
            .doOnComplete(() -> logger.info("Verified that collections exists."))
            .collectList().block();
    }

}