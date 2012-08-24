package dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.hibernate.SessionFactory;

import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.DOMSMetadataExtractor;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.exception.DOMSMetadataExtractionConnectToDOMSException;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.model.BESClippingConfiguration;

public class CommandLineHandler {

    private static final String log4JPropertyFilePathKey = "log4j.config.file.path";
    private static final String hibernatePropertyFilePathKey = "hibernate.config.file.path";
    private static final Logger logger = Logger.getLogger(CommandLineHandler.class);

    public static void main(String[] args) throws FileNotFoundException, IOException, DOMSMetadataExtractionConnectToDOMSException {
        List<String> shardPids = null;
        String propertyFilename = null;
        boolean addJobs = false;
        boolean startJobsInStateToDo = false;
        if ((args.length == 4) && args[0].equalsIgnoreCase("-shardpidfile") && args[2].equalsIgnoreCase("-propfile")) {
            String pidfilename = args[1];
            propertyFilename = args[3];
            shardPids = extractShardPidsFromTextFile(pidfilename);
            addJobs = true; 
        } else if ((args.length == 5) && args[0].equalsIgnoreCase("-shardpidfile") && args[2].equalsIgnoreCase("-execute_jobs") && args[3].equalsIgnoreCase("-propfile")) {
                String pidfilename = args[1];
                propertyFilename = args[4];
                shardPids = extractShardPidsFromTextFile(pidfilename);
                addJobs = true;
                startJobsInStateToDo = true;
        } else if ((args.length == 3) && args[0].equalsIgnoreCase("-execute_jobs") && args[1].equalsIgnoreCase("-propfile")) {
                propertyFilename = args[2];
                startJobsInStateToDo = true;
        } else {
            System.err.println("Missing CLI parameters.");
            printCLIParameters();
            System.exit(1);
        }
        Properties properties = new Properties();
        properties.load(new FileInputStream(propertyFilename));
        // Log4J
        String log4jConfigurationFilename = properties.getProperty(log4JPropertyFilePathKey);
        if (log4jConfigurationFilename == null) {
            throw new RuntimeException("Missing property: " + log4JPropertyFilePathKey +" in file " + propertyFilename); 
        }
        DOMConfigurator.configure(log4jConfigurationFilename);
        // Hibernate
        String hibernateConfigFilePath = properties.getProperty(hibernatePropertyFilePathKey);
        if (hibernateConfigFilePath == null) {
            throw new RuntimeException("Missing property: " + hibernatePropertyFilePathKey  + " in file " + propertyFilename); 
        }
        SessionFactory hibernateSessionFactory = HibernateSessionFactoryFactory.create(hibernateConfigFilePath);
        // JobHandler
        DOMSMetadataExtractor extractor = new DOMSMetadataExtractor(properties);
        BESClippingConfiguration besClippingConfiguration = new BESClippingConfiguration(properties);
        MediaInfoDAO mediaInfoDAO = new MediaInfoDAO(hibernateSessionFactory);
        MediaInfoService mediaInfoService = new MediaInfoService(extractor, besClippingConfiguration , mediaInfoDAO);
        JobDAO jobDAO = new JobDAO(hibernateSessionFactory);
        JobService jobService = new JobService(jobDAO, mediaInfoService);
        if (addJobs) {
            logger.info("Adding jobs from input file.");
            jobService.addJobs(shardPids);
            logger.info("Done adding jobs.");
        } 
        if (startJobsInStateToDo) {
            logger.info("Executing jobs in state todo.");
            jobService.executeJobs();
            logger.info("Done executing jobs.");
        }
    }

    private static void printCLIParameters() {
        System.err.println("Parameter required:");
        System.err.println(" -execute_jobs -propfile <path_to_property_file>");
        System.err.println("");
        System.err.println(" -shardpidfile <filename_of_pid_list> -propfile <path_to_property_file>");
        System.err.println("");
        System.err.println(" -shardpidfile <filename_of_pid_list> -execute_jobs -propfile <path_to_property_file>");
        System.err.println("");
        System.err.println("Shardpidfile : Contains list of shard uuids.");
        System.err.println("Propfile     : Contains path to property file.");
    }

    @SuppressWarnings("unused")
    private static List<String> extractShardPidsFromStdIn() throws IOException {
        List<String> stdInList = new ArrayList<String>();
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String s;
        while ((s = in.readLine()) != null && s.length() != 0) {
            stdInList.add(s);
        }
        return stdInList;
    }

    public static List<String> extractShardPidsFromTextFile(String filename) {
        List<String> shardPids;
        File shardPidFile = new File(filename);
        BufferedReader in = null;
        try {
            in = new BufferedReader(new FileReader(shardPidFile));
        } catch (FileNotFoundException e) {
            System.err.println("Could not find file: " + shardPidFile.getAbsolutePath());
            System.exit(2);
        }
        String shardPid;
        shardPids = new ArrayList<String>(); 
        try {
            while ((shardPid = in.readLine()) != null) {
                shardPids.add(shardPid);
            }
        } catch (IOException e) {
            System.err.println("Could not read file: " + shardPidFile.getAbsolutePath());
            System.exit(3);
        }
        return shardPids;
    }

}
