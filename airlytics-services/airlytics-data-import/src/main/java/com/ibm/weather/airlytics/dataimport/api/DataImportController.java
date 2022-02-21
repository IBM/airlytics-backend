package com.ibm.weather.airlytics.dataimport.api;

import com.ibm.weather.airlytics.dataimport.dto.DataImportConfig;
import com.ibm.weather.airlytics.dataimport.dto.JobStatusReport;
import com.ibm.weather.airlytics.dataimport.integrations.AirlockDataImportClient;
import com.ibm.weather.airlytics.dataimport.services.DataImportService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/dataimport")
@Api(value = "User Features Bulk Data Import REST Endpoint")
public class DataImportController {

    private static final Logger logger = LoggerFactory.getLogger(DataImportController.class);

    @Autowired
    private DataImportService dataImportService;

    @Autowired
    private AirlockDataImportClient airlockClient;

    @ApiOperation(value =
            "Import a CSV from S3",
            response = JobStatusReport.class)
    @ApiResponses(value = {
            @ApiResponse(code = 202, message = "Job Accepted"),
            @ApiResponse(code = 503, message = "Job Failed") })
    @PostMapping(value = "/s3file", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> s3Import(
            @RequestHeader(value = "sessionToken", required = false) String jwt,
            @RequestBody DataImportConfig request) {
        ResponseEntity<Void> response = null;

        if(!airlockClient.validateToken(jwt)) {
            response = ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        } else {

            try {
                JobStatusReport report = new JobStatusReport(
                        JobStatusReport.JobStatus.PENDING,
                        "Pending",
                        null,
                        null,
                        null);
                airlockClient.updateJobStatus(request.getProductId(), request.getUniqueId(), report);
                dataImportService.asyncExecuteJob(request);
                response = ResponseEntity.accepted().build();
            } catch (Exception e) {
                logger.error("Import failed", e);
                response = ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
            }
        }
        return response;
    }

    @ApiOperation(value =
            "Get User Feature table names",
            response = Map.class)
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Success") })
    @GetMapping(value = "/meta/tables", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<String>> getTables(
            @RequestHeader(value = "sessionToken", required = false) String jwt) {
        ResponseEntity<List<String>> response = null;

        if(!airlockClient.validateToken(jwt)) {
            response = ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        } else {

            try {
                return ResponseEntity.ok(dataImportService.getTableNames());
            } catch (Exception e) {
                logger.error("Error obtaining metadata", e);
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
            }
        }
        return response;
    }
}
