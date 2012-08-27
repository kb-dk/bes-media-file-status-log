package dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.xml.DOMConfigurator;
import org.hibernate.SessionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.db.Job;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.exception.JobAlreadyStartedException;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.DOMSMetadataExtractor;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.exception.DOMSMetadataExtractionConnectToDOMSException;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.model.BESClippingConfiguration;

public class JobServiceTest {

    private final Properties properties;

    private final String shardUuid = "uuid:d93054ed-858d-4b2a-870e-b929f5352ad6";//"uuid:abcd786a-73bb-412b-a4c7-433d5fe62d94";
    
    private SessionFactory hibernateSessionFactory;
    public JobServiceTest() throws IOException {
        super();
        File propertyFile = new File("src/test/config/bes_media_file_log_batch_update_unittest.properties");
        FileInputStream in = new FileInputStream(propertyFile);
        properties = new Properties();
        properties.load(in);
        in.close();
        System.getProperties().put("log4j.defaultInitOverride", "true");
        DOMConfigurator.configure(properties.getProperty("log4j.config.file.path"));
    }

    @Before
    public void setUp() throws Exception {
        String hibernateConfigFilePath = properties.getProperty("hibernate.config.file.path");
        this.hibernateSessionFactory = HibernateSessionFactoryFactory.create(hibernateConfigFilePath);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void simpleJobWorkFlow() throws JobAlreadyStartedException, DOMSMetadataExtractionConnectToDOMSException {
        MediaInfoService mediaInfoService = new MediaInfoService(
                new DOMSMetadataExtractor(properties), 
                new BESClippingConfiguration(properties), 
                new MediaInfoDAO(hibernateSessionFactory));
        JobDAO jobDAO = new JobDAO(hibernateSessionFactory);
        JobService jobService = new JobService(jobDAO, mediaInfoService);
        List<String> uuids = new ArrayList<String>();
        uuids.add(shardUuid);
        jobService.addNonExistingJobs(uuids);
        assertEquals(1, jobDAO.getNumberOfAllJobs());
        assertEquals(1, jobDAO.getNumberOfJobsInStateToDo());
        Job job = jobDAO.getJob(shardUuid);
        assertEquals("Todo", job.getStatus());
        jobService.execute(job);
        job = jobDAO.getJob(job.getUuid());
        assertEquals("Done", job.getStatus());
    }

}
