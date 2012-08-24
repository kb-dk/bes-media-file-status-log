package dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
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
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.exception.JobAlreadyStartedException;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.DOMSMetadataExtractor;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.exception.DOMSMetadataExtractionConnectToDOMSException;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.model.BESClippingConfiguration;

public class JobServiceTest {

    private static final DecimalFormat twoPlaces = new DecimalFormat("00");
    private static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static final SimpleDateFormat sdf = new SimpleDateFormat(DATE_PATTERN);

    private final Logger log = Logger.getLogger(MediaInfoServiceTest.class);
    private final Properties properties;

    private final String shardUuid = "uuid:d93054ed-858d-4b2a-870e-b929f5352ad6";//"uuid:abcd786a-73bb-412b-a4c7-433d5fe62d94";
    private final String programMediaFileRelativePath = "src/test/resources/testfiles/programDirectory/d/9/3/0/d93054ed-858d-4b2a-870e-b929f5352ad6.flv";
    private final String previewMediaFileRelativePath = "src/test/resources/testfiles/previewDirectory/d/9/3/0/d93054ed-858d-4b2a-870e-b929f5352ad6.preview.flv";
    private final String[] snapshotMediaFileRelativePath = {
            "src/test/resources/testfiles/snapshotDirectory/d/9/3/0/d93054ed-858d-4b2a-870e-b929f5352ad6.snapshot.0.jpeg",
            "src/test/resources/testfiles/snapshotDirectory/d/9/3/0/d93054ed-858d-4b2a-870e-b929f5352ad6.snapshot.1.jpeg",
            "src/test/resources/testfiles/snapshotDirectory/d/9/3/0/d93054ed-858d-4b2a-870e-b929f5352ad6.snapshot.2.jpeg",
            "src/test/resources/testfiles/snapshotDirectory/d/9/3/0/d93054ed-858d-4b2a-870e-b929f5352ad6.snapshot.3.jpeg",
            "src/test/resources/testfiles/snapshotDirectory/d/9/3/0/d93054ed-858d-4b2a-870e-b929f5352ad6.snapshot.4.jpeg",
            "src/test/resources/testfiles/snapshotDirectory/d/9/3/0/d93054ed-858d-4b2a-870e-b929f5352ad6.snapshot.preview.0.jpeg"
            };
    private final int[] snapshotMediaFileSize = {9720, 17663, 15851, 15490, 8980, 15851};
    
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
        Date testStartedDate = new Date();
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
