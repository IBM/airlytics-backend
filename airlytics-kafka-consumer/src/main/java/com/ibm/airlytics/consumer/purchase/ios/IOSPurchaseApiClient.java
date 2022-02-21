package com.ibm.airlytics.consumer.purchase.ios;

import com.ibm.airlytics.utilities.Constants;
import com.ibm.airlytics.utilities.Environment;
import okhttp3.*;
import org.json.JSONObject;

import javax.annotation.CheckForNull;
import java.io.IOException;

public class IOSPurchaseApiClient {

    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    @SuppressWarnings("FieldCanBeLocal")
    private static final int SWITCH_BOX_ENDPOINT_REQUIRED = 21007;
    public static int USER_ACCOUNT_NOT_FOUND = 21010;

    private static final OkHttpClient client = new OkHttpClient();

    private static final String IOS_SERVICE_ENDPOINT = "https://buy.itunes.apple.com/verifyReceipt";
    private static final String IOS_SEND_BOX_SERVICE_ENDPOINT = "https://sandbox.itunes.apple.com/verifyReceipt";

    private static final String PASSWORD = Environment.getAlphanumericEnv(Constants.IOS_API_PASSWORD, true);


    @CheckForNull
    public String post(String receiptData) throws IOException {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("receipt-data", receiptData);
        jsonObject.put("password", PASSWORD);
        jsonObject.put("exclude-old-transactions", false);
        RequestBody body = RequestBody.create(JSON, jsonObject.toString());
        Request request = new Request.Builder()
                .url(IOS_SERVICE_ENDPOINT)
                .post(body)
                .build();

        Response response = null;

        try {
            response = client.newCall(request).execute();

            if (!response.isSuccessful()) { // in case not 200 return null.
                return null;
            }

            String responseAsString = response.body() == null ? null : response.body().string();

            if (responseAsString != null && responseAsString.length() < 50) {
                Response sendBoxResponse = null;
                try {
                    JSONObject responseAsJSON = new JSONObject(responseAsString);
                    if (responseAsJSON.has("status") && responseAsJSON.getInt("status") == SWITCH_BOX_ENDPOINT_REQUIRED) {
                        // switch to the sandbox verification end point
                        request = new Request.Builder()
                                .url(IOS_SEND_BOX_SERVICE_ENDPOINT)
                                .post(body)
                                .build();
                        sendBoxResponse = client.newCall(request).execute();
                        responseAsString = sendBoxResponse.body() == null ? null : sendBoxResponse.body().string();
                    }
                } catch (Exception e) {
                    return null;
                } finally {
                    if(sendBoxResponse!=null && sendBoxResponse.body()!=null){
                        sendBoxResponse.close();
                    }
                }
            }
            return responseAsString;
        } finally {
            if (response != null && response.body() != null) {
                response.close();
            }
        }
    }
}
