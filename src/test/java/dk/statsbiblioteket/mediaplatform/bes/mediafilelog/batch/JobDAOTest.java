package dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.hibernate.SessionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.db.Job;

public class JobDAOTest {

    private final Logger log = Logger.getLogger(JobDAOTest.class);
    private final Properties defaultProperties;

    private SessionFactory hibernateSessionFactory;
    
    public JobDAOTest() throws IOException {
        super();
        File propertyFile = new File("src/test/config/bes_media_file_log_batch_update_unittest.properties");
        FileInputStream in = new FileInputStream(propertyFile);
        defaultProperties = new Properties();
        defaultProperties.load(in);
        in.close();
        System.getProperties().put("log4j.defaultInitOverride", "true");
        DOMConfigurator.configure(defaultProperties.getProperty("log4j.config.file.path"));
    }

    @Before
    public void setUp() throws Exception {
        String hibernateConfigFilePath = defaultProperties.getProperty("hibernate.config.file.path");
        this.hibernateSessionFactory = HibernateSessionFactoryFactory.create(hibernateConfigFilePath);
    }

    @After
    public void tearDown() throws Exception {
        JobDAO jobDAO = new JobDAO(hibernateSessionFactory);
        List<Job> jobsInDB = jobDAO.getAllJobs();
        log.info("Content of Job-db:");
        log.info("---");
        int i = 0;
        for (Job job : jobsInDB) {
            log.info(i+": " + job);
            i++;
        }
        log.info("---");
    }

    @Test
    public void addJobsToEmptyJobTable() {
        // SessionFactory hibernateSessionFactory = new SessionFactoryMock();
        JobDAO jobDAO = new JobDAO(hibernateSessionFactory);
        int numberOfJobs = jobDAO.getNumberOfAllJobs();
        assertEquals(0, numberOfJobs);
        List<String> uuids = new ArrayList<String>();
        // Alhena uuids:
        uuids.add("uuid:ffdd27de-ac1c-4fe9-9478-a2814313341c");
        uuids.add("uuid:e8f766f3-9990-440a-8ca9-a0de4bd83b3d");
        uuids.add("uuid:7900c7cb-49af-40de-89e7-814d0e563f82");
        jobDAO.addNonExistingJobs(uuids);
        numberOfJobs = jobDAO.getNumberOfAllJobs();
        assertEquals(3, numberOfJobs);
    }

    @Test
    public void jobWorkFlow() {
        //SessionFactory hibernateSessionFactory = new SessionFactoryMock();
        JobDAO jobDAO = new JobDAO(hibernateSessionFactory);
        int numberOfJobs = jobDAO.getNumberOfAllJobs();
        assertEquals("Initial precondition of empte job list is not met.", 0, numberOfJobs);
        List<String> uuids = new ArrayList<String>();
        // Alhena uuids:
        String uuid = "uuid:ffdd27de-ac1c-4fe9-9478-a2814313341c";
        uuids.add(uuid);
        jobDAO.addNonExistingJobs(uuids);
        Job job = jobDAO.getJob(uuid);
        String jobStatus = jobDAO.getJobStatus(uuid);
        Date jobCreatedDate = jobDAO.getJobChangedDate(uuid);
        assertEquals("Todo", jobStatus);
        assertNotNull(jobCreatedDate);
        Job startedJob = jobDAO.startAJob();
        assertEquals(uuid, startedJob.getUuid());
        Date jobStartedDate = jobDAO.getJobChangedDate(uuid);
        assertTrue(jobStartedDate.getTime() > jobCreatedDate.getTime());
        jobStatus = jobDAO.getJobStatus(uuid);
        assertEquals("WIP", jobStatus);
        jobDAO.finishJob(job);
        jobStatus = jobDAO.getJobStatus(uuid);
        assertEquals("Done", jobStatus);
        Date jobFinishedDate = jobDAO.getJobChangedDate(uuid);
        assertTrue(jobFinishedDate.getTime() > jobStartedDate.getTime());
    }

}
