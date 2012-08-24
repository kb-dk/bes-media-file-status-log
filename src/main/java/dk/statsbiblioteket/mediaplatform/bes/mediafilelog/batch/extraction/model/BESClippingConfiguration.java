package dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.model;

import java.util.Properties;


public class BESClippingConfiguration {

    public final String besProgramDirectory;
    public final String besSnapshotDirectory;
    public final String besPreviewDirectory;
    
    public final int videoBitrate;
    public final int audioBitrate;
    
    public final int previewLength;

    public final int snapshotNumber;
    public final int snapshotLength;
    
    public final int startOffsetDigitv;
    public final int endOffsetDigitv;
    public final int startOffsetBart;
    public final int endOffsetBart;
    public final int startOffsetRadio;
    public final int endOffsetRadio;

	public BESClippingConfiguration(Properties properties) {
	    this(
	            properties.getProperty("besProgramDirectory"),
                properties.getProperty("besSnapshotDirectory"),
                properties.getProperty("besPreviewDirectory"),
	            Integer.parseInt(properties.getProperty("besConfiguredVideoBitrate")),
	            Integer.parseInt(properties.getProperty("besConfiguredAudioBitrate")),
	            Integer.parseInt(properties.getProperty("besConfiguredPreviewLength")),
	            Integer.parseInt(properties.getProperty("besConfiguredSnapshotNumber")),
	            Integer.parseInt(properties.getProperty("besConfiguredSnapshotLength")),
	            Integer.parseInt(properties.getProperty("besConfiguredStartOffsetDigitv")),
	            Integer.parseInt(properties.getProperty("besConfiguredEndOffsetDigitv")),
	            Integer.parseInt(properties.getProperty("besConfiguredStartOffsetBart")),
	            Integer.parseInt(properties.getProperty("besConfiguredEndOffsetBart")),
	            Integer.parseInt(properties.getProperty("besConfiguredRadioStartOffset")),
	            Integer.parseInt(properties.getProperty("besConfiguredRadioEndOffset"))
	            );
	}
	
	public BESClippingConfiguration(
	        String besProgramDirectory,
	        String besSnapshotDirectory,
	        String besPreviewDirectory,
	        int videoBitrate, 
	        int audioBitrate, 
	        int previewLength, 
	        int snapshotNumber, 
	        int snapshotLength, 
	        int startOffsetDigitv, 
	        int endOffsetDigitv, 
	        int startOffsetBart, 
	        int endOffsetBart, 
	        int radioStartOffset, 
	        int radioEndOffset) {
		super();
	    this.besProgramDirectory = besProgramDirectory;
	    this.besSnapshotDirectory = besSnapshotDirectory;
	    this.besPreviewDirectory = besPreviewDirectory;
	    this.videoBitrate = videoBitrate;// 400;
	    this.audioBitrate = audioBitrate;// 96;
	    
	    this.previewLength = previewLength;// 30;

	    this.snapshotNumber = snapshotNumber;// 5;
	    this.snapshotLength = snapshotLength;// 10;
	    
	    this.startOffsetDigitv = startOffsetDigitv;// -10;
	    this.endOffsetDigitv = endOffsetDigitv;// 10;
	    this.startOffsetBart = startOffsetBart;// -40;
	    this.endOffsetBart = endOffsetBart;// 40;
	    this.startOffsetRadio = radioStartOffset; // -20
	    this.endOffsetRadio = radioEndOffset; // 20
	}

    public int getStartOffset(MediaTypeEnum mediaType) {
        int startOffset = 0;
        switch (mediaType) {
        case MP3:
            startOffset = new Long(startOffsetRadio).intValue();
            break;
        case WAV:
            startOffset = new Long(startOffsetRadio).intValue();
            break;
        case MPEG:
            startOffset = new Long(startOffsetBart).intValue();
            break;
        case TS:
            startOffset = new Long(startOffsetDigitv).intValue();
            break;
        case FLV:
            startOffset = new Long(startOffsetBart).intValue();
            break;
        default:
            throw new RuntimeException("Unknown start offset for media of type: " + mediaType);
        }
        return startOffset;
    }

    public int getEndOffset(MediaTypeEnum mediaType) {
        int endOffset = 0;
        switch (mediaType) {
        case MP3:
            endOffset = new Long(endOffsetRadio).intValue();
            break;
        case WAV:
            endOffset = new Long(endOffsetRadio).intValue();
            break;
        case MPEG:
            endOffset = new Long(endOffsetBart).intValue();
            break;
        case TS:
            endOffset = new Long(endOffsetDigitv).intValue();
            break;
        case FLV:
            endOffset = new Long(endOffsetBart).intValue();
            break;
        default:
            throw new RuntimeException("Unknown end offset for media of type: " + mediaType);
        }
        return endOffset;
    }

    public long getBesBitRate(MediaTypeEnum mediaType) {
        int bitRate = 0;
        switch (mediaType) {
        case MP3:
            bitRate = audioBitrate;
            break;
        case FLV:
            bitRate = videoBitrate + audioBitrate;
            break;
        default:
            throw new RuntimeException("Unknown bes bitrate for media of type: " + mediaType);
        }
        return bitRate;
    }
}
