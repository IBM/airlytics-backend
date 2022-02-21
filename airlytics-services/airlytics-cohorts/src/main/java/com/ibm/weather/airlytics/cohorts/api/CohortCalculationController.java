package com.ibm.weather.airlytics.cohorts.api;

import com.google.gson.Gson;
import com.ibm.weather.airlytics.cohorts.dto.*;
import com.ibm.weather.airlytics.cohorts.integrations.AirlockCohortsClient;
import com.ibm.weather.airlytics.cohorts.services.CohortCalculationService;
import com.ibm.weather.airlytics.cohorts.services.CohortServiceException;
import com.ibm.weather.airlytics.cohorts.services.AsyncCohortsService;
import com.ibm.weather.airlytics.common.airlock.AirlockException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/cohorts")
@Api(value = "Cohort Calculations REST Endpoint")
public class CohortCalculationController {

    private static final Logger logger = LoggerFactory.getLogger(CohortCalculationController.class);

    @Autowired
    private AsyncCohortsService batchService;

    @Autowired
    private CohortCalculationService calculationService;

    @Autowired
    private AirlockCohortsClient airlockClient;

    @ApiOperation(value = "Recalculate the given cohort", response = BasicJobStatusReport.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Job Completed"),
            @ApiResponse(code = 201, message = "Job Created"),
            @ApiResponse(code = 503, message = "Job Failed") })
    @PostMapping(value = "/{cohort}/execute", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> calculate(
            @RequestHeader(value = "sessionToken", required = false) String jwt,
            @PathVariable( "cohort" ) String cohortId) {
        ResponseEntity<Void> response = null;

        if(!airlockClient.validateToken(jwt)) {
            logger.warn("Unauthorized manual execution for cohort {}", cohortId);
            response = ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        } else {
            logger.info("Manual execution for cohort {}", cohortId);

            try {
                batchService.asyncRunCohortCalculation(cohortId);
                BasicJobStatusReport report = new BasicJobStatusReport(
                        BasicJobStatusReport.JobStatus.PENDING,
                        "Pending",
                        null,
                        0);
                airlockClient.updateJobStatus(cohortId, report);
                response = ResponseEntity.ok().build();
            } catch (Exception e) {
                logger.error("Calculation failed", e);
                response = ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
            }
        }
        return response;
    }

    @ApiOperation(value = "Delete the given cohort from AirCohorts and in third parties", response = BasicJobStatusReport.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Job Completed"),
            @ApiResponse(code = 201, message = "Job Created"),
            @ApiResponse(code = 503, message = "Job Failed") })
    @PostMapping(value = "/{cohort}/delete", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> delete(
            @RequestHeader(value = "sessionToken", required = false) String jwt,
            @PathVariable( "cohort" ) String cohortId,
            @RequestBody CohortDeletionRequest request) {
        ResponseEntity<Void> response = null;

        if(!airlockClient.validateToken(jwt)) {
            logger.warn("Unauthorized deletion for cohort {}", cohortId);
            response = ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        } else {
            logger.info("Deletion request for cohort {}: {}", cohortId,  (new Gson()).toJson(request));

            try {

                if (CollectionUtils.isEmpty(request.getDeletedExports())) {
                    CohortConfig cc = null;

                    try {
                        cc = calculationService.markCohortForDeletion(cohortId);
                    } catch (CohortServiceException e) {
                        logger.error("Error getting Cohort Config for a cohort that is being deleted");
                        response = ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
                        return response;
                    }
                    batchService.asyncCompleteCohortDeletion(cc, request.isDeleteFromThirdParties());
                } else {
                    batchService.asyncCohortDeletionFromThirdParties(request.getProductId(), cohortId, request.getDeletedExports());
                }
                response = ResponseEntity.ok().build();
            } catch (Exception e) {
                logger.error("Deletion failed", e);
                response = ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
            }
        }
        return response;
    }

    @ApiOperation(value =
            "Change cohort's export name",
            response = BasicJobStatusReport.class)
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Success") })
    @PostMapping(value = "/{cohort}/rename", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> rename(
            @RequestHeader(value = "sessionToken", required = false) String jwt,
            @PathVariable( "cohort" ) String cohortId,
            @RequestBody CohortRenameRequest request) {
        ResponseEntity<Void> response = null;

        if(!airlockClient.validateToken(jwt)) {
            logger.warn("Unauthorized rename for cohort {}", cohortId);
            response = ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        } else {
            logger.warn("Rename for cohort {}", cohortId);

            try {
                BasicJobStatusReport report = new BasicJobStatusReport(
                        BasicJobStatusReport.JobStatus.PENDING,
                        "Pending",
                        null,
                        null);
                airlockClient.updateJobStatus(cohortId, report);
                batchService.asyncCohortRename(cohortId, request.getExportKey(), request.getOldExportName(), request.getNewExportName());
                response = ResponseEntity.ok().build();
            } catch (Exception e) {
                logger.error("Renaming failed", e);
                response = ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
            }
        }
        return response;
    }

    @ApiOperation(value =
            "Validate cohort selection SQL condition, and value expression",
            response = BasicJobStatusReport.class)
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Success") })
    @PostMapping(value = "/validate", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<BasicJobStatusReport> validate(
            @RequestHeader(value = "sessionToken", required = false) String jwt,
            @RequestParam(name="limit", required=false, defaultValue="false") boolean limit,
            @RequestBody CohortValidationRequest request) {
        ResponseEntity<BasicJobStatusReport> response = null;

        if(!airlockClient.validateToken(jwt)) {
            logger.warn("Unauthorized validation request for condition {}", request.getQueryCondition());
            response = ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        } else {
            logger.info("Validation request for condition {}", request.getQueryCondition());
            BasicJobStatusReport report = null;

            try {
                report = calculationService.validateCondition(
                        request.getProductId(),
                        request.getJoinedTables(),
                        request.getQueryCondition(),
                        request.getQueryAdditionalValue(),
                        request.getExportType(),
                        limit);
                response = ResponseEntity.ok(report);
            } catch (BadSqlGrammarException e) {
                report = new BasicJobStatusReport(
                        ExportJobStatusReport.JobStatus.FAILED,
                        e.getMessage(),
                        null,
                        null);
                response = ResponseEntity.ok(report);
            } catch (Exception e) {
                logger.error("Validation failed", e);
                report = new BasicJobStatusReport(
                        ExportJobStatusReport.JobStatus.FAILED,
                        String.format("%s: %s", e.getClass().getName(), e.getMessage()),
                        null,
                        null);
                response = ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(report);
            }
        }
        return response;
    }

    @ApiOperation(value =
            "Get Users table column names",
            response = Map.class)
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Success") })
    @GetMapping(value = "/meta/{table}/columns", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Map<String, String>>> getColumns(
            @RequestHeader(value = "sessionToken", required = false) String jwt,
            @PathVariable( "table" ) String table) {

        if(!airlockClient.validateToken(jwt)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        } else {

            try {
                return ResponseEntity.ok(calculationService.getTableColumnNames(table));
            } catch (AirlockException e) {
                logger.error("Error obtaining metadata", e);
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
            }
        }
    }

}
