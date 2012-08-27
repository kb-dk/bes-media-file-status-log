package dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.db.Metadata;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.db.PreviewMediaInfo;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.db.ProgramMediaInfo;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.db.SnapshotMediaInfo;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.DOMSMetadataExtractor;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.exception.DOMSMetadataExtractionConnectToDOMSException;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.model.BESClippingConfiguration;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.model.MediaTypeEnum;
import dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.model.ProgramSearchResultItem;

public class MediaInfoService {

    private static Logger logger = Logger.getLogger(MediaInfoService.class);

    private final DOMSMetadataExtractor extractor;
    private final BESClippingConfiguration besClippingConfiguration;

    private final MediaInfoDAO mediaInfoDAO;

    public MediaInfoService(DOMSMetadataExtractor extractor, 
            BESClippingConfiguration besClippingConfiguration, 
            MediaInfoDAO mediaInfoDAO) {
        super();
        this.extractor = extractor;
        this.besClippingConfiguration = besClippingConfiguration;
        this.mediaInfoDAO = mediaInfoDAO;
    }

    public Metadata retrieveMetadata(String shardUuid) throws DOMSMetadataExtractionConnectToDOMSException {
        ProgramSearchResultItem domsMetadata = extractor.fetchRadioProgramMetadataFromShardPid(shardUuid);
        Metadata metadata = new Metadata();
        metadata.setLastChangedDate(new Date());
        metadata.setShardUuid(shardUuid);
        if (domsMetadata.isExtractionSuccessful()) {
            metadata.setProgramUuid(domsMetadata.getProgram().getProgramPid());
            metadata.setSbChannelID("");
            metadata.setChannelID(domsMetadata.getProgram().getPbcoreProgramMetadata().channel);
            metadata.setProgramTitle(domsMetadata.getProgram().getPbcoreProgramMetadata().titel);
            metadata.setRitzauStartTime(domsMetadata.getProgram().getPbcoreProgramMetadata().start);
            metadata.setRitzauEndTime(domsMetadata.getProgram().getPbcoreProgramMetadata().end);
            // No time to extract file info from DOMS to infer media type
            metadata.setNote("First batch extraction.");
        }
        return metadata;
    }

    public ProgramMediaInfo retrieveProgramMediaInfo(String shardUuid) {
        ProgramMediaInfo programMediaInfo = new ProgramMediaInfo();
        programMediaInfo.setShardUuid(shardUuid);
        String filePath = inferFilePathForProgram(shardUuid);
        if (filePath == null) {
            programMediaInfo.setFileExists(false);
            programMediaInfo.setMediaType(MediaTypeEnum.UNKNOWN);
            programMediaInfo.setFileSizeByte(0);
            programMediaInfo.setFileTimestamp(new Date(0));
            programMediaInfo.setStartOffset(0);
            programMediaInfo.setEndOffset(0);
            programMediaInfo.setLengthInSeconds(0);
            programMediaInfo.setExpectedFileSizeByte(0);
        } else {
            programMediaInfo.setFileExists(true);
            int locationOfFilenameExtension = filePath.lastIndexOf(".");
            String filenameExtension = filePath.substring(locationOfFilenameExtension+1, filePath.length());
            if (filenameExtension.equalsIgnoreCase(MediaTypeEnum.MP3.toString())) {
                programMediaInfo.setMediaType(MediaTypeEnum.MP3);
            } else if (filenameExtension.equalsIgnoreCase(MediaTypeEnum.WAV.toString())) {
                programMediaInfo.setMediaType(MediaTypeEnum.WAV);
            } else if (filenameExtension.equalsIgnoreCase(MediaTypeEnum.JPG.toString())) {
                programMediaInfo.setMediaType(MediaTypeEnum.JPG);
            } else if (filenameExtension.equalsIgnoreCase(MediaTypeEnum.MPEG.toString())) {
                programMediaInfo.setMediaType(MediaTypeEnum.MPEG);
            } else if (filenameExtension.equalsIgnoreCase(MediaTypeEnum.TS.toString())) {
                programMediaInfo.setMediaType(MediaTypeEnum.TS);
            } else if (filenameExtension.equalsIgnoreCase(MediaTypeEnum.FLV.toString())) {
                programMediaInfo.setMediaType(MediaTypeEnum.FLV);
            } else {
                throw new RuntimeException("Could not recognize extension : " + filenameExtension + " for file " + filePath);
            }
            File mediaFile = new File(filePath);
            programMediaInfo.setFileSizeByte(mediaFile.length());
            programMediaInfo.setFileTimestamp(new Date(mediaFile.lastModified()));
            programMediaInfo.setStartOffset(0);
            programMediaInfo.setEndOffset(0);
            programMediaInfo.setLengthInSeconds(0);
            programMediaInfo.setExpectedFileSizeByte(0);
        }
        programMediaInfo.setTranscodeCommandLine("N/A");
        programMediaInfo.setNote("First batch extraction.");
        programMediaInfo.setLastTouched(new Date());
        return programMediaInfo;
    }

    public PreviewMediaInfo retrievePreviewMediaInfo(String shardUuid) {
        PreviewMediaInfo previewMediaInfo = new PreviewMediaInfo();
        previewMediaInfo.setShardUuid(shardUuid);
        String filePath = inferFilePathForPreview(shardUuid);
        if (filePath == null) {
            previewMediaInfo.setFileExists(false);
            previewMediaInfo.setMediaType(MediaTypeEnum.UNKNOWN);
            previewMediaInfo.setFileSizeByte(0);
            previewMediaInfo.setFileTimestamp(new Date(0));
            previewMediaInfo.setStartOffset(0);
            previewMediaInfo.setEndOffset(0);
            previewMediaInfo.setLengthInSeconds(0);
            previewMediaInfo.setExpectedFileSizeByte(0);
        } else {
            previewMediaInfo.setFileExists(true);
            int locationOfFilenameExtension = filePath.lastIndexOf(".");
            String filenameExtension = filePath.substring(locationOfFilenameExtension+1, filePath.length());
            if (filenameExtension.equalsIgnoreCase(MediaTypeEnum.MP3.toString())) {
                previewMediaInfo.setMediaType(MediaTypeEnum.MP3);
            } else if (filenameExtension.equalsIgnoreCase(MediaTypeEnum.WAV.toString())) {
                previewMediaInfo.setMediaType(MediaTypeEnum.WAV);
            } else if (filenameExtension.equalsIgnoreCase(MediaTypeEnum.JPG.toString())) {
                previewMediaInfo.setMediaType(MediaTypeEnum.JPG);
            } else if (filenameExtension.equalsIgnoreCase(MediaTypeEnum.MPEG.toString())) {
                previewMediaInfo.setMediaType(MediaTypeEnum.MPEG);
            } else if (filenameExtension.equalsIgnoreCase(MediaTypeEnum.TS.toString())) {
                previewMediaInfo.setMediaType(MediaTypeEnum.TS);
            } else if (filenameExtension.equalsIgnoreCase(MediaTypeEnum.FLV.toString())) {
                previewMediaInfo.setMediaType(MediaTypeEnum.FLV);
            } else {
                throw new RuntimeException("Could not recognize extension : " + filenameExtension + " for file " + filePath);
            }
            File mediaFile = new File(filePath);
            previewMediaInfo.setFileSizeByte(mediaFile.length());
            previewMediaInfo.setFileTimestamp(new Date(mediaFile.lastModified()));
            previewMediaInfo.setStartOffset(0);
            previewMediaInfo.setEndOffset(0);
            previewMediaInfo.setLengthInSeconds(0);
            previewMediaInfo.setExpectedFileSizeByte(0);
        }
        previewMediaInfo.setTranscodeCommandLine("N/A");
        previewMediaInfo.setNote("First batch extraction.");
        previewMediaInfo.setLastTouched(new Date());
        return previewMediaInfo;
    }

    public List<SnapshotMediaInfo> retrieveSnapshotMediaInfo(String shardUuid) {
        List<SnapshotMediaInfo> snapshotsMediaInfo = new ArrayList<SnapshotMediaInfo>();
        List<String> filePaths = inferFilePathForSnapshots(shardUuid);
        if (filePaths == null) { // No snapshots found
            SnapshotMediaInfo snapshotMediaInfo = new SnapshotMediaInfo();
            snapshotMediaInfo.setShardUuid(shardUuid);
            snapshotMediaInfo.setFileExists(false);
            snapshotMediaInfo.setFilename("");
            snapshotMediaInfo.setFileSizeByte(0);
            snapshotMediaInfo.setFileTimestamp(new Date(0));
            snapshotMediaInfo.setSnapshotTime(0); 
            snapshotMediaInfo.setTranscodeCommandLine("N/A");
            snapshotMediaInfo.setNote("First batch extraction.");
            snapshotMediaInfo.setLastTouched(new Date());
            snapshotsMediaInfo.add(snapshotMediaInfo);
        } else { // At least one file found
            for (String filePath : filePaths) {
                logger.debug("Found file: " + filePath);
                SnapshotMediaInfo snapshotMediaInfo = new SnapshotMediaInfo();
                snapshotMediaInfo.setShardUuid(shardUuid);
                snapshotMediaInfo.setFileExists(true);
                int locationOfLastPathSeparator = filePath.lastIndexOf("/");
                String filename = filePath.substring(locationOfLastPathSeparator+1, filePath.length());
                snapshotMediaInfo.setFilename(filename);
                File mediaFile = new File(filePath);
                snapshotMediaInfo.setFileSizeByte(mediaFile.length());
                snapshotMediaInfo.setFileTimestamp(new Date(mediaFile.lastModified()));
                snapshotMediaInfo.setSnapshotTime(0); // Unknown at this time
                snapshotMediaInfo.setTranscodeCommandLine("N/A");
                snapshotMediaInfo.setNote("First batch extraction.");
                snapshotMediaInfo.setLastTouched(new Date());
                snapshotsMediaInfo.add(snapshotMediaInfo);
            }
        }
        return snapshotsMediaInfo;
}

    public void save(Metadata metadata, 
            ProgramMediaInfo programMediaInfo,
            PreviewMediaInfo previewMediaInfo,
            List<SnapshotMediaInfo> snapshotsMediaInfo) {
        mediaInfoDAO.create(metadata, programMediaInfo, previewMediaInfo, snapshotsMediaInfo);
    }

    /**
     * @return path or null if no matching file was found
     */
    protected String inferFilePathForProgram(String shardUuid) {
        File[] files = getFilePaths(shardUuid, besClippingConfiguration.besProgramDirectory);
        String mediaFilePath = null;
        if (files != null) {
            logger.debug("Found number of files matching uuid: " + files.length
                    + " consisting of the files : " + files);
            if (files.length > 0) {
                mediaFilePath = files[0].getAbsolutePath();
            }
        }
        return mediaFilePath;
    }

    /**
     * @return path or null if no matching file was found
     */
    protected String inferFilePathForPreview(String shardUuid) {
        File[] files = getFilePaths(shardUuid, besClippingConfiguration.besPreviewDirectory);
        String mediaFilePath = null;
        if (files != null) {
            logger.debug("Found number of files matching uuid: " + files.length
                    + " consisting of the files : " + files);
            if (files.length > 0) {
                mediaFilePath = files[0].getAbsolutePath();
            }
        }
        return mediaFilePath;
    }

    /**
     * @return list of paths or null if no matching files where found
     */
    protected List<String> inferFilePathForSnapshots(String shardUuid) {
        File[] files = getFilePaths(shardUuid, besClippingConfiguration.besSnapshotDirectory);
        List<String> mediaFilePaths = null;
        if (files != null) {
            logger.debug("Found number of files matching uuid: " + files.length
                    + " consisting of the files : " + files);
            if (files.length > 0) {
                mediaFilePaths = new ArrayList<String>();
                for (int i=0; i<files.length; i++) {
                    mediaFilePaths.add(files[i].getAbsolutePath());
                }
            }
        }
        logger.debug("Found files: " + mediaFilePaths);
        return mediaFilePaths;
    }

    /**
     * Infer from UUID and a root dir, where to find the corresponding media file in the BES cache
     * @param shardUuid
     * @param besFileTypeRootDir
     * @return
     */
    protected File[] getFilePaths(String shardUuid, String besFileTypeRootDir) {
        Pattern p = Pattern.compile(".*uuid:([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})");
        Matcher m = p.matcher(shardUuid);
        final String shortenedUuid;
        if (!m.matches()) {
            throw new RuntimeException("Uuid did not match expected pattern: " + shardUuid);
        }
        shortenedUuid = m.group(1);
        String subPath = shortenedUuid.substring(0, 1) + File.separator
                + shortenedUuid.substring(1, 2) + File.separator
                + shortenedUuid.substring(2, 3) + File.separator
                + shortenedUuid.substring(3, 4);
        String mediaFileDirectoryPath = besFileTypeRootDir + File.separator
                + subPath; 
        logger.debug("Expected file location: " + mediaFileDirectoryPath);
        File mediaFileDirectoryPathFile = new File(mediaFileDirectoryPath);
        File [] files = mediaFileDirectoryPathFile.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith(shortenedUuid);
            }
        });
        return files;
    }

}
