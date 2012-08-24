package dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.model.Program;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.model.ProgramSearchResultItem;

public class DOMSMetadataExtractorTest {

    private final Logger log = Logger.getLogger(DOMSMetadataExtractorTest.class);
    private static Properties properties;

    private final String shardUuid = "uuid:d93054ed-858d-4b2a-870e-b929f5352ad6";//"uuid:abcd786a-73bb-412b-a4c7-433d5fe62d94";

    @BeforeClass 
    public static void init() throws IOException {
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
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void simpleSearchForShardPids() throws Exception {
        DOMSMetadataExtractor extractor = new DOMSMetadataExtractor(properties);
        List<String> result = extractor.fetchAllShardPids();
        assertTrue("Expecting at least 1 pid.", result.size() > 0);
        String uuid = result.get(0);
        assertEquals("First pid expected to be a specific!", shardUuid, uuid );
    }

    @Test
    public void extractProgramMetadata() throws Exception {
        DOMSMetadataExtractor extractor = new DOMSMetadataExtractor(properties);
        List<String> shardPids = new ArrayList<String>();
        //shardPids.add("uuid:d93054ed-858d-4b2a-870e-b929f5352ad6");
        shardPids.add(shardUuid);
        List<ProgramSearchResultItem> result = extractor.fetchRadioProgramMetadataFromShardPids(shardPids, true);
        Program program = result.get(0).getProgram();
        log.debug("Found metadata of program: " + program);
        assertEquals("Flykatastrofer", program.getPbcoreProgramMetadata().titel);
    }
}
