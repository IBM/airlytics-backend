package com.ibm.airlytics.consumer.purchase.data;

import com.fasterxml.jackson.databind.JsonNode;
import com.ibm.airlytics.consumer.purchase.*;
import com.ibm.airlytics.consumer.purchase.ios.IOSAirlyticsPurchaseEvent;
import com.ibm.airlytics.consumer.purchase.ios.IOSAirlyticsPurchaseState;
import com.ibm.airlytics.consumer.purchase.ios.IOSPurchaseConsumer;
import com.ibm.airlytics.consumer.purchase.ios.IOSubscriptionPurchase;
import com.ibm.airlytics.monitoring.MonitoringServer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import javax.servlet.ServletException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


public class IOSDataRepairer extends IOSPurchaseConsumer {
    MonitoringServer monitoringServer = null;
    private static int BATCH_SIZE = 200;
    private static String SQL = "SQL";
    AtomicInteger index = new AtomicInteger();
    AtomicInteger skipped = new AtomicInteger();
    private ArrayList<AirlyticsPurchase> airlyticsPurchases = new ArrayList<>();
    private PreparedStatement deleteWrongRenewed;
    private PreparedStatement deleteDuplicatedPurchaseEvent;
    private PreparedStatement getPurchasesEventsByID;
    private PreparedStatement updateLastInteractiveRenewalPreparedStatement;
    private PreparedStatement updateInteractiveRenewalPreparedStatement;
    private PreparedStatement updateRenewedByInteractiveRenewalPreparedStatement;


    public IOSDataRepairer(PurchaseConsumerConfig config) {
        super(config, true);


        try {
            getPurchasesEventsByID = PurchasesDAO.getPurchaseEventsByPurchaseIdPreparedStatement(executionConn, config.getPurchasesEventsTable());
            updateLastInteractiveRenewalPreparedStatement = getUpdateLastInteractiveRenewalPreparedStatement(executionConn);
            updateInteractiveRenewalPreparedStatement = getUpdateInteractiveRenewalPreparedStatement(executionConn);
            updateRenewedByInteractiveRenewalPreparedStatement = getUpdateRenewedByInteractiveRenewalPreparedStatement(executionConn);

            starMonitorServer();
            deleteWrongRenewed = getDeleteWrongRenewed(executionConn, config.getPurchasesEventsTable());
            deleteDuplicatedPurchaseEvent = getDeleteWrongDuplicatedPurchaseEventPreparedStatement(executionConn, config.getPurchasesEventsTable());
            preparedStatements.add(updateLastInteractiveRenewalPreparedStatement);
            preparedStatements.add(updateInteractiveRenewalPreparedStatement);
            preparedStatements.add(updateRenewedByInteractiveRenewalPreparedStatement);
        } catch (SQLException | ServletException sqlException) {
            sqlException.printStackTrace();
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
    public static PreparedStatement getUpdateRenewedByInteractiveRenewalPreparedStatement(Connection dbConnection)
            throws SQLException {
        return dbConnection.prepareStatement("update ios_product.purchase_events set purchase_id  = ?, name = 'PURCHASED' where event_time = ? " +
                " and purchase_id = ? and (name='RENEWED' or name='PURCHASED')");

    }

    @SuppressWarnings("SqlResolve")
    public static PreparedStatement getUpdateLastInteractiveRenewalPreparedStatement(Connection dbConnection)
            throws SQLException {
        return dbConnection.prepareStatement("update ios_ios_product.purchase_events set purchase_id  = ? where event_time > ? " +
                " and purchase_id = ?");

    }


    @SuppressWarnings("SqlResolve")
    public static PreparedStatement getDeleteWrongDuplicatedPurchaseEventPreparedStatement(Connection dbConnection, String tableName)
            throws SQLException {
        return dbConnection.prepareStatement("delete  from " + tableName + " where event_time=?" +
                "  and purchase_id=? and name='PURCHASED'");

    }

    @SuppressWarnings("SqlResolve")
    public static PreparedStatement getDeleteWrongRenewed(Connection dbConnection, String tableName)
            throws SQLException {
        return dbConnection.prepareStatement("delete  from " + tableName + " where purchase_id=?" +
                "  and (name='RENEWED' or name='PURCHASED' or name = 'EXPIRED' or name = 'CANCELED')");

    }

    @SuppressWarnings("SqlResolve")
    public static PreparedStatement getUpdateInteractiveRenewalPreparedStatement(Connection dbConnection)
            throws SQLException {
        return dbConnection.prepareStatement("update ios_product.purchase_events set purchase_id  = ? where event_time > ? " +
                " and event_time < ? and purchase_id = ? ");

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
        final List<IOSubscriptionPurchase> ioSubscriptionPurchases = Collections.synchronizedList(new ArrayList<>());
        final List<Pair<StoreResponse, String>> ioSubscriptionPurchasesInvalid = Collections.synchronizedList(new ArrayList<>());
        airlyticsPurchases.forEach(airlyticsPurchase -> {
            airlyticsPurchasesBatch.add(airlyticsPurchase);

            if (airlyticsPurchasesBatch.size() >= BATCH_SIZE) {
                airlyticsPurchasesBatch.stream().parallel().forEach(airlyticsPurchaseParam -> {
                    Pair<IOSubscriptionPurchase, StoreResponse> subscriptionPurchase =
                            getPurchaseDetailsByIOSAPI(airlyticsPurchaseParam.getReceipt());
                    if (subscriptionPurchase.getFirst() != null) {
                        ioSubscriptionPurchases.add(subscriptionPurchase.getFirst());
                    } else {
                        skipped.incrementAndGet();
                        ioSubscriptionPurchasesInvalid.add(new Pair<>(subscriptionPurchase.getSecond(),
                                airlyticsPurchaseParam.getReceipt()));
                    }
                });
                ioSubscriptionPurchases.forEach(ioSubscriptionPurchase -> {
                    index.incrementAndGet();
                    repairedRow(ioSubscriptionPurchase);
                });


                System.out.println("So far processed " + index + " rows");
                try {
                    flushInsertsAndUpdates();
                    commitDB();
                } catch (SQLException sqlException) {
                    sqlException.printStackTrace();
                }

                ioSubscriptionPurchases.clear();
                airlyticsPurchasesBatch.clear();
            }
        });

        airlyticsPurchasesBatch.stream().parallel().forEach(airlyticsPurchaseParam -> {
            Pair<IOSubscriptionPurchase, StoreResponse> subscriptionPurchase =
                    getPurchaseDetailsByIOSAPI(airlyticsPurchaseParam.getReceipt());
            if (subscriptionPurchase.getFirst() != null) {
                ioSubscriptionPurchases.add(subscriptionPurchase.getFirst());
            } else {
                skipped.incrementAndGet();
                ioSubscriptionPurchasesInvalid.add(new Pair<>(subscriptionPurchase.getSecond(),
                        airlyticsPurchaseParam.getReceipt()));
            }
        });
        ioSubscriptionPurchases.forEach(ioSubscriptionPurchase -> {
            index.incrementAndGet();
            repairedRow(ioSubscriptionPurchase);
        });

        // handle invalid rows
        ioSubscriptionPurchasesInvalid.stream().forEach(pair -> {
            repairedRow(pair.getFirst(), pair.getSecond());
        });

        try {
            flushInsertsAndUpdates();
            commitDB();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
        ioSubscriptionPurchasesInvalid.clear();
        airlyticsPurchases.clear();
    }


    private void repairedRow(StoreResponse storeResponse, String receipt) {
        // fixing purchases table
        PurchaseMetaData purchaseMetaData = new PurchaseMetaData();
        purchaseMetaData.setReceipt(receipt);
        purchaseMetaData.setPurchaseToken(receipt);
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

    private void repairedRow(IOSubscriptionPurchase ioSubscriptionPurchase) {


        // fixing purchases table
        PurchaseMetaData purchaseMetaData = new PurchaseMetaData();
        if (ioSubscriptionPurchase.getLatestReceipt() != null) {
            purchaseMetaData.setReceipt(ioSubscriptionPurchase.getLatestReceipt());
            purchaseMetaData.setPlatform(getPlatform());
            purchaseMetaData.setType(PurchaseMetaData.Type.APP);
            ArrayList<AirlyticsPurchaseEvent> accumulativeInteractiveRenewalEvents = new ArrayList<>();
            Collection<AirlyticsPurchase> updatedPurchasesWithFixedInteractiveRenewal = new ArrayList<>();
            Collection<AirlyticsPurchase> updatedPurchases = processReceiptFromPurchase(ioSubscriptionPurchase.clone(),
                    purchaseMetaData);


            for (AirlyticsPurchase updatedAirlyticsPurchase : updatedPurchases) {
                try {
                    //get all available purchase events
                    List<AirlyticsPurchaseEvent> purchaseEvents =
                            PurchasesDAO.getPurchaseEventsByPurchaseId(getPurchasesEventsByID, updatedAirlyticsPurchase.getId());

                    List<AirlyticsPurchaseEvent> interactiveRenewalEvents =
                            getInteractiveRenewalEvents(purchaseEvents.stream().filter(e -> !e.getName().equals("RENEWAL_STATUS_CHANGED")).collect(Collectors.toList()));
                    accumulativeInteractiveRenewalEvents.addAll(interactiveRenewalEvents);

                    if (interactiveRenewalEvents != null) {
                        updatedPurchasesWithFixedInteractiveRenewal.addAll(new IOSAirlyticsPurchaseState(ioSubscriptionPurchase.clone(),
                                purchaseMetaData,
                                inAppAirlyticsProducts, interactiveRenewalEvents).getUpdatedAirlyticsPurchase());
                    }
                } catch (Exception e) {
                    logException(e, "Error in processRecords, " + e.getMessage());
                } finally {
                }
            }

            // fixing purchase events table
            IOSAirlyticsPurchaseEvent airlyticsPurchaseEvent1 = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase.clone(),
                    purchaseMetaData,
                    inAppAirlyticsProducts
            );

            if (accumulativeInteractiveRenewalEvents.isEmpty()) {

                String currentPurchaseId;
                String previousPurchaseId = null;
                for (AirlyticsPurchaseEvent event : airlyticsPurchaseEvent1.getPurchaseEvents()) {
                    currentPurchaseId = event.getPurchaseId();
                    try {
                        if (previousPurchaseId == null || !previousPurchaseId.equals(currentPurchaseId)) {
                            deleteWrongRenewed.setString(1, currentPurchaseId);
                            deleteWrongRenewed.addBatch();
                        }
                        previousPurchaseId = currentPurchaseId;
                        PurchasesDAO.updatePurchaseEvent(
                                purchaseEventUpdateStatement, event);
                    } catch (SQLException sqlException) {
                        logException(sqlException, "Error in insertion purchase event, " + sqlException.getMessage());
                    }
                }

                for (AirlyticsPurchase updatedAirlyticsPurchase : updatedPurchases) {
                    try {
                        PurchasesDAO.insertUpdatePurchase(purchaseStatement, updatedAirlyticsPurchase);
                    } catch (Exception e) {
                        logException(e, "Error in processRecords, " + e.getMessage());
                    } finally {
                    }
                }
            } else {
                // sort interactive event by days (ascending)
                // override the compare() method
                Collections.sort(accumulativeInteractiveRenewalEvents, (Comparator) (o1, o2) -> {
                    if (o1 instanceof AirlyticsPurchaseEvent && o2 instanceof AirlyticsPurchaseEvent) {
                        AirlyticsPurchaseEvent s_1 = (AirlyticsPurchaseEvent) o1;
                        AirlyticsPurchaseEvent s_2 = (AirlyticsPurchaseEvent) o2;

                        return s_1.getEventTime().compareTo(s_2.getEventTime());
                    }
                    return 0;
                });


                AirlyticsPurchaseEvent current = null;
                AirlyticsPurchaseEvent previous = null;
                for (AirlyticsPurchaseEvent airlyticsPurchaseEvent : accumulativeInteractiveRenewalEvents) {
                    try {
                        current = airlyticsPurchaseEvent;

                        if (previous != null) {

                            String newPurchaseId = getInteractivePurchaseIdByTime(previous.getEventTime(), updatedPurchasesWithFixedInteractiveRenewal);
                            deleteWrongRenewed.setString(1, newPurchaseId);
                            deleteWrongRenewed.execute();


                            deleteDuplicatedPurchaseEvent.setTimestamp(1, new Timestamp(previous.getEventTime()));
                            deleteDuplicatedPurchaseEvent.setString(2, previous.getPurchaseId());
                            deleteDuplicatedPurchaseEvent.execute();


                            updateRenewedByInteractiveRenewalPreparedStatement(previous, updatedPurchasesWithFixedInteractiveRenewal);

                            updateInteractiveRenewalPreparedStatement.setString(1, newPurchaseId);
                            updateInteractiveRenewalPreparedStatement.setTimestamp(2, new Timestamp(previous.getEventTime()));
                            updateInteractiveRenewalPreparedStatement.setTimestamp(3, new Timestamp(current.getEventTime()));
                            updateInteractiveRenewalPreparedStatement.setString(4, previous.getPurchaseId());
                            updateInteractiveRenewalPreparedStatement.addBatch();
                        } else {
                            previous = current;
                        }
                    } catch (Exception e) {
                        logException(e, "Error in processRecords, " + e.getMessage());
                    }
                }
                if (current != null) {
                    try {

                        String newPurchaseId = getInteractivePurchaseIdByTime(current.getEventTime(), updatedPurchasesWithFixedInteractiveRenewal);
                        deleteWrongRenewed.setString(1, newPurchaseId);
                        deleteWrongRenewed.execute();

                        deleteDuplicatedPurchaseEvent.setTimestamp(1, new Timestamp(current.getEventTime()));
                        deleteDuplicatedPurchaseEvent.setString(2, current.getPurchaseId());
                        deleteDuplicatedPurchaseEvent.execute();


                        updateRenewedByInteractiveRenewalPreparedStatement(current, updatedPurchasesWithFixedInteractiveRenewal);

                        updateLastInteractiveRenewalPreparedStatement.setString(1,
                                getInteractivePurchaseIdByTime(current.getEventTime(), updatedPurchasesWithFixedInteractiveRenewal));
                        updateLastInteractiveRenewalPreparedStatement.setTimestamp(2, new Timestamp(current.getEventTime()));
                        updateLastInteractiveRenewalPreparedStatement.setString(3, current.getPurchaseId());
                        updateLastInteractiveRenewalPreparedStatement.addBatch();
                        updateLastInteractiveRenewalPreparedStatement.executeBatch();
                    } catch (Exception e) {
                        logException(e, "Error in processRecords, " + e.getMessage());
                    }
                }

                for (AirlyticsPurchase updatedAirlyticsPurchase : updatedPurchasesWithFixedInteractiveRenewal) {
                    try {
                        PurchasesDAO.insertUpdatePurchase(purchaseStatement, updatedAirlyticsPurchase);
                    } catch (Exception e) {
                        logException(e, "Error in processRecords, " + e.getMessage());
                    } finally {
                    }
                }
            }
        }
    }

    private void updateRenewedByInteractiveRenewalPreparedStatement(AirlyticsPurchaseEvent renewal,
                                                                    Collection<AirlyticsPurchase> updatedPurchasesWithFixedInteractiveRenewal) throws SQLException {
        updateRenewedByInteractiveRenewalPreparedStatement.setString(1,
                getInteractivePurchaseIdByTime(renewal.getEventTime(), updatedPurchasesWithFixedInteractiveRenewal));
        updateRenewedByInteractiveRenewalPreparedStatement.setTimestamp(2, new Timestamp(renewal.getEventTime()));
        updateRenewedByInteractiveRenewalPreparedStatement.setString(3, renewal.getPurchaseId());
        updateRenewedByInteractiveRenewalPreparedStatement.addBatch();
    }

    private String getInteractivePurchaseIdByTime(long startDate, Collection<AirlyticsPurchase> updatedPurchasesWithFixedInteractiveRenewal) {
        return updatedPurchasesWithFixedInteractiveRenewal.stream().filter(t -> t.getStartDate().equals(startDate)).findFirst().get().getId();
    }

    private List<AirlyticsPurchaseEvent> getInteractiveRenewalEvents(Collection<AirlyticsPurchaseEvent> purchaseEvents) {
        AirlyticsPurchaseEvent previous = null;
        ArrayList<AirlyticsPurchaseEvent> interactiveRenewalEvents = new ArrayList<>();

        for (AirlyticsPurchaseEvent purchaseEvent : purchaseEvents) {
            if ((purchaseEvent.getName().equals(PurchaseEventType.RENEWED.name()) || purchaseEvent.getName().equals(PurchaseEventType.PURCHASED.name()))
                    && previous != null && previous.getName().equals(PurchaseEventType.EXPIRED.name())) {
                interactiveRenewalEvents.add(purchaseEvent.clone());
            }
            previous = purchaseEvent;
        }
        return interactiveRenewalEvents;
    }

    private void addRepairedRowToBatch(AirlyticsPurchase airlyticsPurchase) {
        airlyticsPurchases.add(airlyticsPurchase);
        if (airlyticsPurchases.size() % 1000 == 0) {
            System.out.println("So far read " + airlyticsPurchases.size() + " rows");
        }
    }
}
