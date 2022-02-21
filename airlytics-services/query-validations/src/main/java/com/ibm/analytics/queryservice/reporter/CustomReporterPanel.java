package com.ibm.analytics.queryservice.reporter;

import com.ibm.analytics.queryservice.queryHandler.AthenaAWSQueryHandler;
import org.testng.ISuite;
import org.testng.Reporter;
import org.testng.reporters.XMLStringBuffer;
import org.testng.reporters.jq.Model;
import org.testng.reporters.jq.ReporterPanel;

import java.util.List;
import java.util.Map;

public class CustomReporterPanel extends ReporterPanel {

    public final static String FIELD_SEPARATOR = ";";

    private String title;
    private String capitalizedTitle;

    @Override
    public String getPrefix() {
        return title + "-";
    }

    public CustomReporterPanel(Model model, String title) {
        super(model);
        this.title = title;
        capitalizedTitle = title.substring(0, 1).toUpperCase() + title.substring(1);
    }

    @Override
    public String getContent(ISuite suite, XMLStringBuffer main) {
        Map parameters = suite.getXmlSuite().getAllParameters();
        String day = AthenaAWSQueryHandler.getDayAgoString(Integer.valueOf(String.valueOf(parameters.get("daysBefore"))), false);
        XMLStringBuffer xsb = new XMLStringBuffer(main.getCurrentIndent());
        StringBuilder sb = new StringBuilder();
        sb.append("<style>\n" +
                "tr\n" +
                "{border-left:1px solid black;\n" +
                "border-top:1px solid black;}\n" +
                "td\n" +
                "{border-left:1px solid black;\n" +
                "border-top:1px solid black;}\n" +
                "th\n" +
                "{border-left:1px solid black;\n" +
                "border-top:1px solid black;}\n" +
                "table\n" +
                "{border-right:1px solid black;\n" +
                "border-bottom:1px solid black;}\n" +
                "</style>");
        sb.append("<h2>" + capitalizedTitle + " report [Events date: " + day + "] </h2>");
        sb.append("<table id=\"" + title + title + "\">");
        if (title.equals("info")) {
            sb.append("<tr><th>Platform</th><th onclick=\"sortTable(1,'" + title + title +"')\">AppVersion</th><th>Event Name</th><th>Number of events</th><th>Details</th></tr>");
        } else {
            sb.append("<tr><th>Platform</th><th onclick=\"sortTable(1,'" + title + title +"')\">AppVersion</th><th>Test Name</th><th>Number of results</th><th>Expected results</th><th>Details</th></tr>");
        }
        xsb.addString(sb.toString());

            List<String> lines = Reporter.getOutput();
            if (!lines.isEmpty()) {
                for (String line : lines) {
                    String[] columns = line.split(FIELD_SEPARATOR);
                    for (int i = 0; i < columns.length; i++) {
                        if (i == 0) {
                            String columnContent = columns[0];
                            if (columnContent.equals(ReporterMessage.Levels.info.name()) && !title.equals(ReporterMessage.Levels.info.name())) {
                                break;
                            } else if (!columnContent.equals(ReporterMessage.Levels.info.name()) && title.equals(ReporterMessage.Levels.info.name())) {
                                break;
                            } else if (columnContent.equals(ReporterMessage.Levels.test.name()) && title.equals(ReporterMessage.Levels.error.name())) {
                                break;
                            }
                            if (columnContent.equals(ReporterMessage.Levels.error.name())) {
                                xsb.addString("<tr bgcolor=\"pink\">");
                            }else{
                                xsb.addString("<tr>");
                            }
                        } else if (i == columns.length - 1) {
                            xsb.addString( "<td><button type=\"button\" onclick=\"alert('" + columns[i] + "')\">show</button></td>");
                            xsb.addString("</tr>");
                        } else {
                            xsb.addString( "<td>" + columns[i] + "</td>");
                        }
                    }
                }
            }

        xsb.addString("</table>");
        return xsb.toXML();
    }

    @Override
    public String getNavigatorLink(ISuite suite) {
        return capitalizedTitle + " Report";
    }
}
