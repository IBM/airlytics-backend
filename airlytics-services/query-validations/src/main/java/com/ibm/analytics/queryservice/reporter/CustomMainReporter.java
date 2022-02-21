package com.ibm.analytics.queryservice.reporter;

import org.testng.IReporter;
import org.testng.ISuite;
import org.testng.internal.Utils;
import org.testng.reporters.Files;
import org.testng.reporters.XMLStringBuffer;
import org.testng.reporters.jq.*;
import org.testng.xml.XmlSuite;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import static org.testng.reporters.jq.BasePanel.C;
import static org.testng.reporters.jq.BasePanel.D;

public class CustomMainReporter implements IReporter {
    private static final String TESTNG_RESOURCE_PREFIX = "/org/testng/";
    private static final String[] RESOURCES = new String[] {
            "jquery-1.7.1.min.js", "testng-reports.css", "testng-reports.js",
            "passed.png", "failed.png", "skipped.png", "navigator-bullet.png",
            "bullet_point.png", "collapseall.gif"
    };
    private static final String[] CUSTOM_RESOURCES = new String[] {
            "utils.js"
    };

    public static final String REPORT_HEADER_FILE = "header";

    private Model m_model;
    private String m_outputDirectory;

    @Override
    public void generateReport(List<XmlSuite> xmlSuites, List<ISuite> suites,
                               String outputDirectory) {
        m_model = new Model(suites);
        m_outputDirectory = outputDirectory;

        XMLStringBuffer xsb = new XMLStringBuffer("    ");

        // Generate the top banner
        new BannerPanel(m_model).generate(xsb);

        // All the panels selectable from the navigator
        List<INavigatorPanel> panels = Arrays.<INavigatorPanel>asList(
                new TestNgXmlPanel(m_model),
                new TestPanel(m_model),
                new GroupPanel(m_model),
                new TimesPanel(m_model),
                new CustomReporterPanel(m_model, ReporterMessage.Levels.error.name()),
                new CustomReporterPanel(m_model, ReporterMessage.Levels.test.name()),
                new CustomReporterPanel(m_model, ReporterMessage.Levels.info.name()),
                new IgnoredMethodsPanel(m_model),
                new ChronologicalPanel(m_model));

        // Generate the navigator on the left hand side
        new NavigatorPanel(m_model, panels).generate(xsb);

        xsb.push(D, C, "wrapper");
        xsb.push(D, "class", "main-panel-root");

        //
        // Generate the main suite panel
        //
        new SuitePanel(m_model).generate(xsb);

        // Generate all the navigator panels
        for (INavigatorPanel panel : panels) {
            panel.generate(xsb);
        }

        xsb.pop(D); // main-panel-root
        xsb.pop(D); // wrapper
        for (String fileName : CUSTOM_RESOURCES) {
            xsb.addString("<script src=\"" + fileName + "\"></script>\n");
        }
        xsb.addString("  </body>\n");
        xsb.addString("</html>\n");

        String all;
        try {
            try (InputStream header = getClass().getResourceAsStream(TESTNG_RESOURCE_PREFIX + REPORT_HEADER_FILE)) {
                if (header == null) {
                    throw new RuntimeException("Couldn't find resource header");
                }
                for (String fileName : RESOURCES) {
                    String prefix = TESTNG_RESOURCE_PREFIX;
                    if (fileName.startsWith("#LOCAL#")){
                        prefix = "";
                        fileName = fileName.substring(7);
                    }
                    try (InputStream is = getClass().getResourceAsStream(prefix + fileName)) {
                        if (is == null) {
                            throw new AssertionError("Couldn't find resource: " + fileName);
                        }
                        Files.copyFile(is, new File(m_outputDirectory, fileName));
                    }
                }
                for (String fileName : CUSTOM_RESOURCES) {
                    try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName)) {
                        if (is == null) {
                            throw new AssertionError("Couldn't find resource: " + fileName);
                        }
                        Files.copyFile(is, new File(m_outputDirectory, fileName));
                    }
                }

                all = Files.readFile(header);
                //TODO add utils to header that resides on header
                Utils.writeUtf8File(m_outputDirectory, "index.html", xsb, all);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}

