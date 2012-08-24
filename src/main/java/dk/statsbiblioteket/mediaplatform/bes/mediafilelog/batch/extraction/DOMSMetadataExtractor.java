package dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.namespace.QName;
import javax.xml.ws.BindingProvider;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import dk.statsbiblioteket.doms.central.CentralWebservice;
import dk.statsbiblioteket.doms.central.CentralWebserviceService;
import dk.statsbiblioteket.doms.central.InvalidCredentialsException;
import dk.statsbiblioteket.doms.central.InvalidResourceException;
import dk.statsbiblioteket.doms.central.MethodFailedException;
import dk.statsbiblioteket.doms.central.RecordDescription;
import dk.statsbiblioteket.doms.central.Relation;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.exception.DOMSMetadataExtractionConnectToDOMSException;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.exception.DOMSMetadataExtractionException;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.exception.DOMSMetadataExtractionParseFilenameException;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.exception.DOMSMetadataExtractionParsePBCoreException;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.exception.DOMSMetadataExtractionUnknownMediaTypeException;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.model.BESClippingConfiguration;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.model.MediaTypeEnum;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.model.PBCoreProgramMetadata;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.model.Program;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.model.ProgramSearchResultItem;
import dk.statsbiblioteket.util.xml.DOM;
import dk.statsbiblioteket.util.xml.XPathSelector;

/**
 * This class is responsible for extracting metadata from DOMS
 * 
 * @author heb@statsbiblioteket.dk
 *
 */
public class DOMSMetadataExtractor {
	
	static Logger logger = Logger.getLogger(DOMSMetadataExtractor.class);
    
	private static final QName CENTRAL_WEBSERVICE_SERVICE = new QName(
    		"http://central.doms.statsbiblioteket.dk/",
    		"CentralWebserviceService");
    private CentralWebservice domsService;

	private BESClippingConfiguration besConfiguration;

	private final String domsBaseUrl;
    private final String domsUserName;
    private final String domsPassword;


	/**
	 * Used at runtime
	 * 
	 * @param properties
	 */
	public DOMSMetadataExtractor(Properties properties) {
		super();
		this.domsBaseUrl = properties.getProperty("domsBaseUrl");
		String domsWSAPIEndpointUrlString = properties.getProperty("domsWSAPIEndpointUrlString");
		domsUserName = properties.getProperty("userName");
		domsPassword = properties.getProperty("password");
		domsService = createDOMSCentralWebService(domsWSAPIEndpointUrlString, domsUserName, domsPassword);
		this.besConfiguration = new BESClippingConfiguration(properties);
	}
	
	public List<String> fetchAllShardPids()
			throws InvalidCredentialsException, MethodFailedException,
			InvalidResourceException, IOException {
		List<String> shardPids = new ArrayList<String>();
        URL domsSearchURL = new URL(domsBaseUrl + "/risearch?query=select%20%24x%20%0Afrom%20%3C%23ri%3E%0Awhere%20%24x%20%3Cinfo%3Afedora%2Ffedora-system%3Adef%2Fmodel%23hasModel%3E%20%3Cinfo%3Afedora%2Fdoms%3AContentModel_Shard%3E%20minus%20%24x%20%3Cinfo%3Afedora%2Ffedora-system%3Adef%2Fmodel%23state%3E%20%3Cinfo%3Afedora%2Ffedora-system%3Adef%2Fmodel%23Deleted%3E%20minus%20%24x%20%3Chttp%3A%2F%2Fecm.sourceforge.net%2Frelations%2F0%2F2%2F%23isTemplateFor%3E%20%24y&lang=itql&format=csv&limit=0");
		URLConnection uc = domsSearchURL.openConnection();
		String userpass = domsUserName + ":" + domsPassword;
		String basicAuth = "Basic " + new String(new Base64().encode(userpass.getBytes())).replaceAll("\n", "");
		logger.debug("Resulting auth: " + basicAuth);
		uc.setRequestProperty ("Authorization", basicAuth);
	    BufferedReader in = new BufferedReader(new InputStreamReader(uc.getInputStream()));
	    in.readLine(); // First line is heading
	    String inputLine;
	    while ((inputLine = in.readLine()) != null) {
	        Pattern p = Pattern.compile(".*(uuid:[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})");
	        Matcher m = p.matcher(inputLine);
	        if (m.matches()) {
	            shardPids.add(m.group(1));
	        } else {
	            logger.warn("Output line from DOMS did not match expected pattern: " + inputLine);
	        }
	        logger.debug("Read line: " + inputLine + ". Parsed line: " + m.group(1));
	    }
	    in.close();
		return shardPids;
	}

	/**
	 * Fetches metadata of radio programs that have been changed sinde the date given as argument.
	 * 
	 * @param updatedSince
	 * @return
	 * @throws DOMSMetadataExtractionException
	 */
	public List<ProgramSearchResultItem> fetchRadioProgramMetadataUpdatedSince(Date updatedSince) throws DOMSMetadataExtractionConnectToDOMSException {
		List<RecordDescription> updatedShardPids = fetchUpdatedShardPids(updatedSince);
		List<ProgramSearchResultItem> radioProgramMetadataList = fetchRadioProgramMetadataFromShardPids(updatedShardPids);  
		return radioProgramMetadataList;
	}

	protected List<RecordDescription> fetchUpdatedShardPids(Date updatedSince) throws DOMSMetadataExtractionConnectToDOMSException {
		List<RecordDescription> updatedShardPids = new ArrayList<RecordDescription>();
		int pageOffset = 0;
		int pageSize = 10000;
		boolean continueToNextPage = true;
		while (continueToNextPage) {
			List<RecordDescription> newPidsInPage = fetchUpdatedShardPidsPaged(updatedSince, pageOffset*pageSize, pageSize);
			updatedShardPids.addAll(newPidsInPage);
			pageOffset++;
			continueToNextPage = !newPidsInPage.isEmpty();
		}
		return updatedShardPids;
	}

	protected List<RecordDescription> fetchUpdatedShardPids(Date updatedSince, int maxNumberToReturn) throws DOMSMetadataExtractionConnectToDOMSException {
		List<RecordDescription> updatedShardPids = new ArrayList<RecordDescription>();
		int pageOffset = 0;
		int pageSize = 10000;
		boolean continueToNextPage = true;
		while (continueToNextPage) {
			int remaningPageSize = Math.min(pageSize, (pageOffset*pageSize - maxNumberToReturn));
			int offset = pageOffset*pageSize;
			List<RecordDescription> newPidsInPage = fetchUpdatedShardPidsPaged(updatedSince, offset, remaningPageSize);
			updatedShardPids.addAll(newPidsInPage);
			pageOffset++;
			continueToNextPage = (!newPidsInPage.isEmpty() || (maxNumberToReturn < pageOffset*pageSize));
		}
		return updatedShardPids;
	}

	/**
	 * Get shard pids modified since given date. Filter out pids not being shards.
	 * 
	 * @param updatedSince
	 * @param offset
	 * @param pageSize
	 * @return
	 * @throws DOMSMetadataExtractionConnectToDOMSException
	 */
	protected List<RecordDescription> fetchUpdatedShardPidsPaged(Date updatedSince, int offset, int pageSize) throws DOMSMetadataExtractionConnectToDOMSException {
		List<RecordDescription> newShardPidsInPage = new ArrayList<RecordDescription>();
		try {
			List<RecordDescription> domsRecords = domsService.getIDsModified(updatedSince.getTime(), "doms:RadioTV_Collection", "BES", "Published", offset, pageSize);
			logger.info("Page " + offset + " containing " + domsRecords.size() + " records");
		    for (RecordDescription record: domsRecords) {
		    	String recordS = "Found updated pid = '" + record.getPid()
		                + "' Entry CM = '" + record.getEntryContentModelPid()
		                + "' Date = " + (new Date(record.getDate()));
		        //logger.debug(recordS);
		        if ("doms:ContentModel_Shard".equals(record.getEntryContentModelPid())) {
		        	newShardPidsInPage.add(record);
		        } else {
		        	logger.warn("Record with unexpted contentModel: " + recordS);
		        }
		    }
		} catch (InvalidCredentialsException e) {
			throw new DOMSMetadataExtractionConnectToDOMSException("Failed to connect to DOMS.", e);
		} catch (MethodFailedException e) {
			throw new DOMSMetadataExtractionConnectToDOMSException("Failed to connect to DOMS.", e);
		}
		return newShardPidsInPage;
	}

	public List<String> fetchUpdatedShardPidsPagedAsString(Date updatedSince, int offset, int pageSize) throws DOMSMetadataExtractionConnectToDOMSException {
		List<String> shardPids = new ArrayList<String>();
		List<RecordDescription> searchResult = fetchUpdatedShardPidsPaged(updatedSince, offset, pageSize);
		for (RecordDescription recordDescription : searchResult) {
			shardPids.add(recordDescription.getPid());
		}
		return shardPids ;
	}

	/**
	 * 
	 * @param recordDescription
	 * @return List of lists. First list contains successful exported and second list contains failed.
	 * @throws DOMSMetadataExtractionConnectToDOMSException If the connection to DOMS failed
	 * @throws InvalidCredentialsException
	 * @throws InvalidResourceException
	 * @throws MethodFailedException
	 * @throws ParseException
	 */
	public List<ProgramSearchResultItem> fetchRadioProgramMetadataFromShardPids(List<RecordDescription> recordDescription) throws DOMSMetadataExtractionConnectToDOMSException  {
		ArrayList<ProgramSearchResultItem> searchResults = new ArrayList<ProgramSearchResultItem>();
		int i=0;
		for (RecordDescription searchResultRecord : recordDescription) {
			ProgramSearchResultItem radioProgramSearchResult = null;
			try {
				logger.info("Handling shard: " + searchResultRecord + " - " + i + "/ " + recordDescription.size());
				Program radioProgram = new Program(searchResultRecord.getPid(), searchResultRecord.getDate(), besConfiguration);
				radioProgramSearchResult = fetchRadioProgramMetadataFromShardPid(radioProgram);
			} catch (DOMSMetadataExtractionException e) {
				logger.warn("Unable to extract radio program: " + e.getMessage(), e);
				Program radioProgram = new Program(searchResultRecord.getPid(), searchResultRecord.getDate());
				radioProgramSearchResult = new ProgramSearchResultItem(radioProgram);
				radioProgramSearchResult.extractionFailed("Unable to extract metadata.");
			}
			searchResults.add(radioProgramSearchResult);
			i++;
		}
		return searchResults;
	}
	
	/**
	 * 
	 * @param shardPids
	 * @param useThisMethod Ignored. Only used so the runtime system can differentiate the method signature from similar method (See java type erasure)
	 * @return
	 * @throws DOMSMetadataExtractionConnectToDOMSException 
	 */
	public List<ProgramSearchResultItem> fetchRadioProgramMetadataFromShardPids(List<String> shardPids, boolean useThisMethod) throws DOMSMetadataExtractionConnectToDOMSException {
		ArrayList<ProgramSearchResultItem> searchResults = new ArrayList<ProgramSearchResultItem>();
		int i=0;
		for (String shardPid: shardPids) {
		    logger.info(i + "/ " + shardPids.size());
			ProgramSearchResultItem radioProgramSearchResult = fetchRadioProgramMetadataFromShardPid(shardPid);
			searchResults.add(radioProgramSearchResult);
			i++;
		}
		return searchResults;
	}

    public ProgramSearchResultItem fetchRadioProgramMetadataFromShardPid(
            String shardPid)
            throws DOMSMetadataExtractionConnectToDOMSException {
        ProgramSearchResultItem radioProgramSearchResult = null;
        logger.info("Handling shard: " + shardPid);
        try {
        	Program radioProgram = new Program(shardPid, besConfiguration);
        	radioProgramSearchResult = fetchRadioProgramMetadataFromShardPid(radioProgram);
        } catch (DOMSMetadataExtractionException e) {
        	logger.warn("Unable to extract radio program: " + e.getMessage(), e);
        	Program radioProgram = new Program(shardPid);
        	radioProgramSearchResult = new ProgramSearchResultItem(radioProgram);
        	radioProgramSearchResult.extractionFailed("Unable to extract metadata.");
        }
        return radioProgramSearchResult;
    }

	protected ProgramSearchResultItem fetchRadioProgramMetadataFromShardPid(Program program) throws DOMSMetadataExtractionConnectToDOMSException, DOMSMetadataExtractionException {
		try {
			ProgramSearchResultItem searchResult = new ProgramSearchResultItem(program);
			// Handle shard metadata
			String shardMetadata = domsService.getDatastreamContents(program.shardPid, "SHARD_METADATA");
			logger.trace(" - related shard metadata:\n " + shardMetadata);
			// Handle program metadata
			List<Relation> relationsToShard = domsService.getInverseRelations(program.shardPid);
			if (relationsToShard.size() != 1) {
				throw new DOMSMetadataExtractionException("Unexpected number of relations to the shard with pid: " + program.shardPid);
			}
			Relation shardRelation = relationsToShard.get(0);
			String programPid = shardRelation.getSubject();
			logger.trace("   - related program: " + programPid);
			String programPBCore = domsService.getDatastreamContents(programPid, "PBCORE");
			logger.trace(programPBCore);
			try {
			    program.setProgramPid(programPid);
				PBCoreProgramMetadata pbcoreProgramMetadata = extractMetadataFromPBCore(program.shardPid, programPBCore);
				program.setPbcoreProgramMetadata(pbcoreProgramMetadata);
				logger.debug("Found program: " + pbcoreProgramMetadata.titel);
			} catch (DOMSMetadataExtractionParsePBCoreException e) {
				logger.warn(program.shardPid + " - Could not extract metadata from PBCore", e);
				searchResult.extractionFailed("Could not extract metadata from PBCore");
			}
			if (!searchResult.validate()) {
				logger.warn(program.shardPid + " - Could not validate search result.");
			}
			return searchResult;
		} catch (InvalidCredentialsException e) {
			logger.error("Invalid configuration. Stopping extraction.", e);
			throw new DOMSMetadataExtractionConnectToDOMSException("Invalid configuration. Stopping extraction.", e);
		} catch (InvalidResourceException e) {
			logger.error("Invalid configuration. Stopping extraction.", e);
			throw new DOMSMetadataExtractionConnectToDOMSException("Invalid configuration. Stopping extraction.", e);
		} catch (MethodFailedException e) {
			logger.error("Invalid configuration. Stopping extraction.", e);
			throw new DOMSMetadataExtractionConnectToDOMSException("Invalid configuration. Stopping extraction.", e);
		}
	}

	protected PBCoreProgramMetadata extractMetadataFromPBCore(String shardPid, String programPBCore) throws DOMSMetadataExtractionParsePBCoreException {
		String channel;
		String titleTitel;
		String titleOriginaltitel;
		String titleEpisodetitel;
		Date dateAvailableStart;
		Date dateAvailableEnd;
		String creatorForfattere;
		String contributerMedvirkende;
		String contributerInstruktion;
		String descriptionKortOmtale;
		String descriptionLangOmtale1;
		String descriptionLangOmtale2;
		XPathSelector xpath = DOM.createXPathSelector("pb", "http://www.pbcore.org/PBCore/PBCoreNamespace.html");
		Document doc = DOM.stringToDOM(programPBCore, true);
		
		//channel = getChannelId(programPBCore);
		channel = xpath.selectString(doc, "/pb:PBCoreDescriptionDocument/pb:pbcorePublisher[pb:publisherRole='channel_name']/pb:publisher");
		logger.debug("Parsed channel: " + channel);
		
		//titleTitel = getProgramTitleFromPBCore(programPBCore);
		titleTitel = xpath.selectString(doc, "/pb:PBCoreDescriptionDocument/pb:pbcoreTitle[pb:titleType='titel']/pb:title");
		logger.debug("Parsed titleTitel: " + titleTitel);

		//titleOriginaltitel = getTitleOriginaltitelFromPBCore(programPBCore);
		titleOriginaltitel = xpath.selectString(doc, "/pb:PBCoreDescriptionDocument/pb:pbcoreTitle[pb:titleType='originaltitel']/pb:title");
		logger.debug("Parsed titleOriginaltitel: " + titleOriginaltitel);
		
		//titleEpisodetitel = getTitleEpisodetitelFromPBCore(programPBCore);
		titleEpisodetitel = xpath.selectString(doc, "/pb:PBCoreDescriptionDocument/pb:pbcoreTitle[pb:titleType='episodetitel']/pb:title");
		logger.debug("Parsed titleEpisodetitel: " + titleEpisodetitel);
		
		dateAvailableStart = getStartDateFromPBCore(programPBCore);
		dateAvailableEnd = getEndDateFromPBCore(programPBCore);

		//descriptionKortOmtale = getDescriptionKortOmtaleFromPBCore(programPBCore);
		descriptionKortOmtale = xpath.selectString(doc, "/pb:PBCoreDescriptionDocument/pb:pbcoreDescription[pb:descriptionType='kortomtale']/pb:description");
		logger.debug("Parsed descriptionKortOmtale: " + descriptionKortOmtale);
		
		//descriptionLangOmtale1 = getDescriptionLangOmtale1FromPBCore(programPBCore);
		descriptionLangOmtale1 = xpath.selectString(doc, "/pb:PBCoreDescriptionDocument/pb:pbcoreDescription[pb:descriptionType='langomtale1']/pb:description");
		logger.debug("Parsed descriptionLangOmtale1: " + descriptionLangOmtale1);
		
		//descriptionLangOmtale2 = getDescriptionLangOmtale2FromPBCore(programPBCore);
		descriptionLangOmtale2 = xpath.selectString(doc, "/pb:PBCoreDescriptionDocument/pb:pbcoreDescription[pb:descriptionType='langomtale2']/pb:description");
		logger.debug("Parsed descriptionLangOmtale2: " + descriptionLangOmtale2);

		//creatorForfattere = getCreatorForfattereFromPBCore(programPBCore);
		creatorForfattere = xpath.selectString(doc, "/pb:PBCoreDescriptionDocument/pb:pbcoreCreator[pb:creatorRole='forfatter']/pb:creator");
		logger.debug("Parsed creatorForfattere: " + creatorForfattere);
		
		//contributerMedvirkende = getContributorMedvirkendeFromPBCore(programPBCore);
		contributerMedvirkende = xpath.selectString(doc, "/pb:PBCoreDescriptionDocument/pb:pbcoreContributor[pb:contributorRole='medvirkende']/pb:contributor");
		logger.debug("Parsed contributerMedvirkende: " + contributerMedvirkende);
		
		//contributerInstruktion = getContributorInstruktionFromPBCore(programPBCore);
		contributerInstruktion = xpath.selectString(doc, "/pb:PBCoreDescriptionDocument/pb:pbcoreContributor[pb:contributorRole='instruktion']/pb:contributor");
		logger.debug("Parsed contributerInstruktion: " + contributerInstruktion);
		
		PBCoreProgramMetadata pbcoreProgramMetadata = new PBCoreProgramMetadata( 
				channel, 
				titleTitel,
				titleOriginaltitel,
				titleEpisodetitel,
				dateAvailableStart, 
				dateAvailableEnd, 
				descriptionKortOmtale,
				descriptionLangOmtale1,
				descriptionLangOmtale2,
				creatorForfattere,
				contributerMedvirkende,
				contributerInstruktion);
		return pbcoreProgramMetadata;
	}
	
	protected MediaTypeEnum extractMediaTypeFromFilename(String filename) throws DOMSMetadataExtractionUnknownMediaTypeException, DOMSMetadataExtractionParseFilenameException {
		Pattern p = Pattern.compile(".*\\.(.*)", Pattern.DOTALL);
		Matcher m = p.matcher(filename);
		if (!m.find()) {
			throw new DOMSMetadataExtractionParseFilenameException("Could not parse filename: " + filename);
		}
		String filenameExtension = m.group(1);
		MediaTypeEnum type = null;
		if ("wav".equals(filenameExtension)) {
			type = MediaTypeEnum.WAV;
		} else if ("mp3".equals(filenameExtension)) {
			type = MediaTypeEnum.MP3;
		} else if ("jpg".equals(filenameExtension)) {
			type = MediaTypeEnum.JPG;
		} else if ("mpeg".equals(filenameExtension)) {
			type = MediaTypeEnum.MPEG;
		} else if ("ts".equals(filenameExtension)) {
			type = MediaTypeEnum.TS;
		} else if ("flv".equals(filenameExtension)) {
			type = MediaTypeEnum.FLV;
		} else {
			throw new DOMSMetadataExtractionUnknownMediaTypeException("Unknown media type of file: " + filename + ". " +
					"Infered extension was: " + filenameExtension);
		}
		return type;
	}

	protected static String getChannelId(String programPBCore) {
		XPathSelector xpath = DOM.createXPathSelector("pb", "http://www.pbcore.org/PBCore/PBCoreNamespace.html");
		Document doc = DOM.stringToDOM(programPBCore, true);
		String channelID = xpath.selectString(doc, "/pb:PBCoreDescriptionDocument/pb:pbcorePublisher[pb:publisherRole='channel_name']/pb:publisher");
		logger.debug("Parsed channelID: " + channelID);
		return channelID; //extractStringContent(programPBCore, "<pbcorePublisher>.*<publisher>(.*)</publisher>.*<publisherRole>channel_name</publisherRole>.*</pbcorePublisher>");
	}

	private static Date getStartDateFromPBCore(String programPBCore) throws DOMSMetadataExtractionParsePBCoreException {
		XPathSelector xpath = DOM.createXPathSelector("pb", "http://www.pbcore.org/PBCore/PBCoreNamespace.html");
		Document doc = DOM.stringToDOM(programPBCore, true);
		String startDateString = xpath.selectString(doc, "/pb:PBCoreDescriptionDocument/pb:pbcoreInstantiation/pb:pbcoreDateAvailable/pb:dateAvailableStart");
		logger.debug("Parsed startDate: " + startDateString);
        // Format example 2008-05-09T18:00:00+0200
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZZZZZ");
        Date date;
		try {
			date = formatter.parse(startDateString);
		} catch (ParseException e) {
			logger.warn("Unable to parse start date '" + startDateString + "' from PBCore.\n" + programPBCore );
			throw new DOMSMetadataExtractionParsePBCoreException("Unable to parse start date " + startDateString + " from PBCore.", e);
		}
		return date;
	}

	private static Date getEndDateFromPBCore(String programPBCore) throws DOMSMetadataExtractionParsePBCoreException {
		XPathSelector xpath = DOM.createXPathSelector("pb", "http://www.pbcore.org/PBCore/PBCoreNamespace.html");
		Document doc = DOM.stringToDOM(programPBCore, true);
		String endDateString = xpath.selectString(doc, "/pb:PBCoreDescriptionDocument/pb:pbcoreInstantiation/pb:pbcoreDateAvailable/pb:dateAvailableEnd");
		logger.debug("Parsed endDate: " + endDateString);
        // Format example 2008-05-09T18:00:00+0200
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZZZZZ");
        Date date;
		try {
			date = formatter.parse(endDateString);
		} catch (ParseException e) {
			logger.warn("Unable to parse end date " + endDateString + " from PBCore.\n" + programPBCore );
			throw new DOMSMetadataExtractionParsePBCoreException("Unable to parse end date " + endDateString + " from PBCore.", e);
		}
		return date;
	}

	private CentralWebservice createDOMSCentralWebService(
			String domsWSAPIEndpointUrlString, String userName, String password) {
		logger.debug("Creating DOMS Client");
		logger.debug("domsWSAPIEndpointUrlString, " + domsWSAPIEndpointUrlString);
		logger.debug("userName: " + userName);
		logger.debug("password: " + password);
		URL domsWSAPIEndpoint;
		try {
			domsWSAPIEndpoint = new URL(domsWSAPIEndpointUrlString);
		} catch (MalformedURLException e) {
			throw new RuntimeException("URL to DOMS not configured correctly. Was: " + domsWSAPIEndpointUrlString, e);
		}
		CentralWebservice domsAPI = new CentralWebserviceService(domsWSAPIEndpoint, CENTRAL_WEBSERVICE_SERVICE).getCentralWebservicePort();
		Map<String, Object>  domsAPILogin = ((BindingProvider) domsAPI).getRequestContext();
		domsAPILogin.put(BindingProvider.USERNAME_PROPERTY, userName);
		domsAPILogin.put(BindingProvider.PASSWORD_PROPERTY, password);
		return domsAPI;
	}
}
