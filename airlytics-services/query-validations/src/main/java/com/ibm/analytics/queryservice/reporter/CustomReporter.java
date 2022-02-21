package com.ibm.analytics.queryservice.reporter;

import com.ibm.analytics.queryservice.queryHandler.AthenaAWSQueryHandler;
import org.testng.*;
import org.testng.internal.Utils;
import org.testng.reporters.SuiteHTMLReporter;
import org.testng.xml.XmlSuite;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CustomReporter extends SuiteHTMLReporter {
    public final static String FIELD_SEPARATOR = ";";

    @Override
    public void generateReport(List<XmlSuite> xmlSuites, List<ISuite> suites,
                               String outputDirectory) {
        String[] headers = {"Info", "Errors", "Tests"};
        for (ISuite suite : suites) {
            Map parameters = suite.getXmlSuite().getAllParameters();
            String day = AthenaAWSQueryHandler.getDayAgoString(Integer.valueOf(String.valueOf(parameters.get("daysBefore"))), false);
            StringBuilder csvOutput = new StringBuilder();
            csvOutput.append("Status,Platform,AppVersion,TestName,#Results,#Expected\n");
            StringBuilder[] sb = new StringBuilder[3];
            for (int i = 0; i < 3; i++) {
                sb[i] = new StringBuilder();
                sb[i].append("<style>\n" +
                        "tr\n" +
                        "{border-left:1px solid black;\n" +
                        "border-top:1px solid black;}\n" +
                        "td\n" +
                        "{border-left:1px solid black;\n" +
                        "border-top:1px solid black;}\n" +
                        "table\n" +
                        "{border-right:1px solid black;\n" +
                        "border-bottom:1px solid black;}\n" +
                        "</style>");
                sb[i].append("<h2>" + headers[i] + " report [Events date: " + day + "] </h2>")
                        .append("<table>");
                if (i == 0) {
                    sb[i].append("<tr bgcolor=\"grey\"><td>Platform</td><td>AppVersion</td><td>Event Name</td><td>Number of events</td><td>Details</td></tr>");

                } else {
                    sb[i].append("<tr bgcolor=\"grey\"><td>Platform</td><td>AppVersion</td><td>Test Name</td><td>Number of results</td><td>Expected results</td><td>Details</td></tr>");
                }
            }
            List<String> output = Reporter.getOutput();
            for (String line : output) {
                String[] columns = line.split(FIELD_SEPARATOR);
                List<Integer> outputDestination = new ArrayList<>();
                String lineInfo = "<tr>";
                for (int i = 0; i < columns.length; i++) {
                    if (i == 0) {
                        String columnContent = columns[0];
                        if (columnContent.equals(ReporterMessage.Levels.error.name())) {
                            csvOutput.append("error,");
                            outputDestination.add(1);
                            outputDestination.add(2);
                            lineInfo = "<tr bgcolor=\"pink\">";
                        } else if (columnContent.equals(ReporterMessage.Levels.info.name())) {
                            csvOutput.append("info,");
                            outputDestination.add(0);
                        } else if (columnContent.equals(ReporterMessage.Levels.test.name())) {
                            csvOutput.append("success,");
                            outputDestination.add(2);
                        }
                    } else if (i < columns.length - 1){
                        lineInfo = "<td>" + columns[i] + "</td>";
                        if (i < columns.length -2){
                            csvOutput.append(columns[i].replaceAll(",",""));
                            csvOutput.append(",");
                        }
                        if (i == columns.length -2){
                            csvOutput.append(columns[i].replaceAll(",",""));
                            csvOutput.append("\n");
                        }
                    } else if (i == columns.length - 1) {
                        lineInfo = "<td><button type=\"button\" onclick=\"alert('" + columns[i] + "')\">show</button></td></tr>";
                    }
                    for (int destination : outputDestination) {
                        sb[destination].append(lineInfo);
                    }
                }
            }

            for (int i = 0; i < 3; i++) {
                sb[i].append("</table>");
            }
            Utils.writeFile(outputDirectory + File.separator + day, "reportInfo.html", sb[0].toString());
            Utils.writeFile(outputDirectory + File.separator + day, "reportError.html", sb[1].toString());
            Utils.writeFile(outputDirectory + File.separator + day, "reportTests.html", sb[2].toString());
            Utils.writeFile(outputDirectory , "csv-report-" + day + ".csv", csvOutput.toString());
        }
    }
}
