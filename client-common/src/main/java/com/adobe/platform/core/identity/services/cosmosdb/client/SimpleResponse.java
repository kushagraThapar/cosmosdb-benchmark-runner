package com.adobe.platform.core.identity.services.cosmosdb.client;

import com.azure.data.cosmos.FeedResponseDiagnostics;

import java.util.Collections;
import java.util.List;

public class SimpleResponse {
    List<SimpleDocument> documents;
    int statusCode;
    double ruUsed;
    double requestLatencyInMillis;
    String activityId;
    FeedResponseDiagnostics feedResponseDiagnostics;

    public SimpleResponse(List<SimpleDocument> documents, int statusCode, double ruUsed, double requestLatencyInMillis, String activityId) {
        this.documents = documents;
        this.statusCode = statusCode;
        this.ruUsed = ruUsed;
        this.requestLatencyInMillis = requestLatencyInMillis;
        this.activityId = activityId;
    }

    public SimpleResponse(SimpleDocument document, int statusCode, double ruUsed, double requestLatencyInMillis, String activityId) {
       this(Collections.singletonList(document), statusCode, ruUsed, requestLatencyInMillis, activityId);
    }

    public List<SimpleDocument> getDocuments() {
        return documents;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public double getRuUsed() {
        return ruUsed;
    }

    public double getRequestLatencyInMillis() {
        return requestLatencyInMillis;
    }

    public String getActivityId() {
        return activityId;
    }

    public FeedResponseDiagnostics getFeedResponseDiagnostics() {
        return feedResponseDiagnostics;
    }

    public void setFeedResponseDiagnostics(FeedResponseDiagnostics feedResponseDiagnostics) {
        this.feedResponseDiagnostics = feedResponseDiagnostics;
    }
}
