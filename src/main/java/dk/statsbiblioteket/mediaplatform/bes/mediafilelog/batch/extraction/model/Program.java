package dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.model;

import java.util.Date;

public class Program implements Comparable<Program> {

	//private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public final String shardPid;
	public String programPid;
    public PBCoreProgramMetadata pbcoreProgramMetadata;
	private BESClippingConfiguration besConfiguration;
	private long updatedInDOMSDate;
    private MediaTypeEnum mediaType;
    public Program(String shardPid) {
		super();
		this.shardPid = shardPid;
	}

	public Program(String shardPid, BESClippingConfiguration besConfiguration) {
		this(shardPid);
		this.besConfiguration = besConfiguration;
	}

	public Program(String shardPid, long updatedInDOMSDate) {
		this(shardPid);
		this.updatedInDOMSDate = updatedInDOMSDate;
	}

	public Program(String shardPid, long updatedInDOMSDate, BESClippingConfiguration besConfiguration) {
		this(shardPid, updatedInDOMSDate);
		this.besConfiguration = besConfiguration;
	}

	public Date getStartTime() {
	    int startOffset = besConfiguration.getStartOffset(this.mediaType);
	    return new Date(pbcoreProgramMetadata.start.getTime()+startOffset);
	}
	
    public Date getEndTime() {
        int endOffset = besConfiguration.getEndOffset(this.mediaType);
        return new Date(pbcoreProgramMetadata.end.getTime()+endOffset);
    }
    
	public long getUpdatedInDOMSDate() {
		return updatedInDOMSDate;
	}
	
	public PBCoreProgramMetadata getPbcoreProgramMetadata() {
		return pbcoreProgramMetadata;
	}

	public void setPbcoreProgramMetadata(PBCoreProgramMetadata pbcoreProgramMetadata) {
		this.pbcoreProgramMetadata = pbcoreProgramMetadata;
	}

	public void setProgramPid(String programPid) {
        this.programPid = programPid;
    }

    public String getProgramPid() {
        return this.programPid;
    }

    public String getPresentationFilename() {
		return shardPid.substring(5, shardPid.length()) + ".mp3";
	}

	public String toString() {
		String s = this.getClass().getSimpleName()
			+ " [uuid : " + shardPid 
			+ ", pbcoreMetadata : " + pbcoreProgramMetadata;
		s += "]";
		return s;
	}

	@Override
	public int compareTo(Program other) {
		int result;
		if (this.pbcoreProgramMetadata.start == null) {
			if (other.pbcoreProgramMetadata.start == null) {
				result = 0;
			} else {
				result = -1; // null is less than anything not-null
			}
		} else {
			if (other.pbcoreProgramMetadata.start == null) {
				result = 1; // anything not-null is larger than null
			} else {
				result = this.pbcoreProgramMetadata.start.compareTo(other.pbcoreProgramMetadata.start);
			}
		}
		return result;
	}
}
