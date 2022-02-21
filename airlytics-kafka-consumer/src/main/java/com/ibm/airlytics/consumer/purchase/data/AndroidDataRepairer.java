package com.ibm.airlytics.consumer.purchase.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.api.services.androidpublisher.model.SubscriptionPurchase;
import com.ibm.airlytics.consumer.purchase.AirlyticsPurchase;
import com.ibm.airlytics.consumer.purchase.AirlyticsPurchaseEvent;
import com.ibm.airlytics.consumer.purchase.PurchaseConsumerConfig;
import com.ibm.airlytics.consumer.purchase.PurchasesDAO;
import com.ibm.airlytics.consumer.purchase.android.AndroidAirlyticsPurchaseEvent;
import com.ibm.airlytics.consumer.purchase.android.AndroidPurchaseConsumer;
import com.ibm.airlytics.monitoring.MonitoringServer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import javax.servlet.ServletException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;


public class AndroidDataRepairer extends AndroidPurchaseConsumer {
    MonitoringServer monitoringServer = null;
    private static int BATCH_SIZE = 200;
    private static String SQL = "SQL";
    AtomicInteger index = new AtomicInteger();
    AtomicInteger skipped = new AtomicInteger();
    private ArrayList<AirlyticsPurchase> airlyticsPurchases = new ArrayList<>();
    private PreparedStatement deleteWrongRenewed;


    public AndroidDataRepairer(PurchaseConsumerConfig config) {
        super(config, true);

        try {
            starMonitorServer();
            deleteWrongRenewed = getDeleteWrongRows(executionConn, config.getPurchasesEventsTable());
            preparedStatements.add(deleteWrongRenewed);
        } catch (SQLException | ServletException sqlException) {
            sqlException.printStackTrace();
        }


        try {
            fetchInAppsList(System.getenv(APP_PACKAGE_NAME_ENV_VARIABLE));
        } catch (Exception e) {
            logException(e, "Error in fetching InApp Products list");
            stop();
        }

        try {
            repairerRows();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }


        System.out.println("Done " + index.get() + " rows");
        System.out.println("Skipped " + skipped.get() + " rows");
        System.exit(0);
    }


    private void starMonitorServer() throws ServletException {
        int monitoringPort = 8084;
        monitoringServer = new MonitoringServer(this);
        monitoringServer.start(monitoringPort);
    }

    @Override
    protected int processRecords(ConsumerRecords<String, JsonNode> records) {
        return 0;
    }

    /**
     * The query is hard-coded and will be used to fix a specific bug in the existing data
     *
     * @param dbConnection
     * @param tableName
     * @return
     * @throws SQLException
     */
    @SuppressWarnings("SqlResolve")
    public static PreparedStatement getPurchasesShouldBeUpdatedOncePreparedStatement(Connection dbConnection, String tableName)
            throws SQLException {
        System.out.println("SQL:" + System.getenv(SQL));
        return dbConnection.prepareStatement(System.getenv(SQL));
    }

    @SuppressWarnings("SqlResolve")
    public static PreparedStatement getDeleteWrongRows(Connection dbConnection, String tableName)
            throws SQLException {
        return dbConnection.prepareStatement("delete  from " + tableName + " where purchase_id=?" +
                "  and name = 'EXPIRED'");
    }

    private void repairerRows() {
        try {
            PreparedStatement fetchByMonth = getPurchasesShouldBeUpdatedOncePreparedStatement(longReadQueryConn, config.getPurchasesTable());
            PurchasesDAO.notifyPurchaseCandidatesForUpdate(
                    fetchByMonth,
                    dbOperationsLatencySummary, pollingCommandsCounter, "fixed", this::addRepairedRowToBatch);
        } catch (SQLException e) {
            logException(e, "Error in repairerRows,details: " + e.getMessage());
        }
        ArrayList<AirlyticsPurchase> airlyticsPurchasesBatch = new ArrayList<>();
        final List<Pair<SubscriptionPurchase, AirlyticsPurchase>> androidSubscriptionPurchases = Collections.synchronizedList(new ArrayList<>());
        final List<Pair<StoreResponse, String>> androidSubscriptionPurchasesInvalid = Collections.synchronizedList(new ArrayList<>());
        airlyticsPurchases.forEach(airlyticsPurchase -> {
            airlyticsPurchasesBatch.add(airlyticsPurchase);

            if (airlyticsPurchasesBatch.size() >= BATCH_SIZE) {
                airlyticsPurchasesBatch.stream().parallel().forEach(airlyticsPurchaseParam -> {
                    Pair<SubscriptionPurchase, StoreResponse> subscriptionPurchase =
                            queryPurchaseDetails(PurchaseMetaData.Type.APP.name(), airlyticsPurchaseParam.getId(),
                                    airlyticsPurchaseParam.getProduct(), 0, System.getenv(APP_PACKAGE_NAME_ENV_VARIABLE));
                    if (subscriptionPurchase.getFirst() != null) {
                        androidSubscriptionPurchases.add(new Pair(subscriptionPurchase.getFirst(), airlyticsPurchaseParam));
                    } else {
                        skipped.incrementAndGet();
                        androidSubscriptionPurchasesInvalid.add(new Pair<>(subscriptionPurchase.getSecond(),
                                airlyticsPurchaseParam.getReceipt()));
                    }
                });
                androidSubscriptionPurchases.forEach(subscriptionPurchase -> {
                    index.incrementAndGet();
                    try {
                        repairedRow(subscriptionPurchase.getFirst(), subscriptionPurchase.getSecond().getProduct(),
                                subscriptionPurchase.getSecond().getId());
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                });


                System.out.println("So far processed " + index + " rows");
                try {
                    flushInsertsAndUpdates();
                    commitDB();
                } catch (SQLException sqlException) {
                    sqlException.printStackTrace();
                }

                androidSubscriptionPurchases.clear();
                airlyticsPurchasesBatch.clear();
            }

        });

        airlyticsPurchasesBatch.stream().parallel().forEach(airlyticsPurchaseParam -> {
            Pair<SubscriptionPurchase, StoreResponse> subscriptionPurchase =
                    queryPurchaseDetails(PurchaseMetaData.Type.APP.name(), airlyticsPurchaseParam.getId(),
                            airlyticsPurchaseParam.getProduct(), 0, "app.package");
            if (subscriptionPurchase.getFirst() != null) {
                androidSubscriptionPurchases.add(new Pair(subscriptionPurchase.getFirst(), airlyticsPurchaseParam));
            } else {
                skipped.incrementAndGet();
                androidSubscriptionPurchasesInvalid.add(new Pair<>(subscriptionPurchase.getSecond(),
                        airlyticsPurchaseParam.getId()));
            }
        });
        androidSubscriptionPurchases.forEach(subscriptionPurchase -> {
            index.incrementAndGet();
            try {
                repairedRow(subscriptionPurchase.getFirst(), subscriptionPurchase.getSecond().getProduct(),
                        subscriptionPurchase.getSecond().getId());
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        });

        // handle invalid rows
        androidSubscriptionPurchasesInvalid.stream().forEach(pair -> {
            repairedRow(pair.getFirst(), pair.getSecond());
        });

        try {
            flushInsertsAndUpdates();
            commitDB();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
        androidSubscriptionPurchasesInvalid.clear();
        airlyticsPurchases.clear();
    }


    private void repairedRow(StoreResponse storeResponse, String id) {
        // fixing purchases table
        PurchaseMetaData purchaseMetaData = new PurchaseMetaData();
        purchaseMetaData.setReceipt(id);
        purchaseMetaData.setPurchaseToken(id);
        purchaseMetaData.setPlatform(getPlatform());
        purchaseMetaData.setType(PurchaseMetaData.Type.APP);

        try {
            PurchasesDAO.setPurchaseInActiveAndUpdateStoreResponse(purchaseStoreResponseUpdateStatement,
                    purchaseMetaData, storeResponse);
        } catch (Exception e) {
            logException(e, "Error in processRecords, " + e.getMessage());
        } finally {
        }
    }

    private void repairedRow(SubscriptionPurchase subscriptionPurchase, String productId, String id) throws JsonProcessingException {
        // fixing purchases table
        PurchaseMetaData purchaseMetaData = new PurchaseMetaData();
        AirlyticsPurchaseEvent upgradedSub = null;
        if (subscriptionPurchase != null) {
            purchaseMetaData.setPlatform(getPlatform());
            purchaseMetaData.setPurchaseToken(id);
            purchaseMetaData.setProductId(productId);
            purchaseMetaData.setType(PurchaseMetaData.Type.APP);
            purchaseMetaData.setTransactionId(id);


            try {

                List<AirlyticsPurchaseEvent> historicalPurchaseEvents = PurchasesDAO.getPurchaseEventsByPurchaseId(getPurchaseEventsByID, id);
                AirlyticsPurchase updatedPurchase = processPurchase(subscriptionPurchase.clone(),
                        purchaseMetaData, historicalPurchaseEvents);
                updatedPurchase.setId(id);
                PurchasesDAO.insertUpdatePurchase(purchaseStatement, updatedPurchase);

                if (subscriptionPurchase.getLinkedPurchaseToken() != null) {
                    Optional<AirlyticsPurchaseEvent> upgradedSubOptional = PurchasesDAO.getPurchaseEventsByPurchaseId(getPurchaseEventsByID,
                            subscriptionPurchase.getLinkedPurchaseToken()).
                            stream().filter(event -> event.getName().equals(PurchaseEventType.UPGRADED.name())).findFirst();

                    upgradedSub = upgradedSubOptional.isPresent() ? upgradedSubOptional.get() : null;

                    if (upgradedSub != null) {
                        upgradedSub.setUpgradedTo(purchaseMetaData.getPurchaseToken());
                    } else {
                        LOGGER.warn("PurchaseToken:" + subscriptionPurchase.getLinkedPurchaseToken() + " has not UPGRADED event");
                    }
                }

                // fixing purchase events table
                AndroidAirlyticsPurchaseEvent airlyticsPurchaseEvent = new AndroidAirlyticsPurchaseEvent(
                        subscriptionPurchase.clone(),
                        purchaseMetaData,
                        inAppAirlyticsProducts.get(purchaseMetaData.getProductId()),
                        updatedPurchase);


                List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = airlyticsPurchaseEvent.getPurchaseEvents();

                if (upgradedSub != null) {
                    airlyticsPurchaseEvents.add(upgradedSub);
                }

                for (AirlyticsPurchaseEvent event : airlyticsPurchaseEvents) {
                    try {
                        PurchasesDAO.updatePurchaseEvent(
                                purchaseEventUpdateStatement, event);
                    } catch (SQLException sqlException) {
                        logException(sqlException, "Error in insertion purchase event, " + sqlException.getMessage());
                    }
                }
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
            }
        }

    }

    private void addRepairedRowToBatch(AirlyticsPurchase airlyticsPurchase) {
        airlyticsPurchases.add(airlyticsPurchase);
        if (airlyticsPurchases.size() % 1000 == 0) {
            System.out.println("So far read " + airlyticsPurchases.size() + " rows");
        }
    }
}
