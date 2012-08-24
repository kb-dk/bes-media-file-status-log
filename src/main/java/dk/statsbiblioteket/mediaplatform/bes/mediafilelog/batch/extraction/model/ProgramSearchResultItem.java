package dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.model;

import java.util.ArrayList;
import java.util.List;

public class ProgramSearchResultItem {

	private Program program;
	
	private boolean isFilteredOut;
	private boolean isExtractionFailed;
	private List<String> statusMessages;

	private ProgramSearchResultItem() {
		super();
		this.isFilteredOut = false;
		this.isExtractionFailed = false;
		this.statusMessages = new ArrayList<String>();
	}
	
	public ProgramSearchResultItem(Program radioProgramMetadata) {
		this();
		this.program = radioProgramMetadata;
	}

	public Program getProgram() {
		return program;
	}
	
	public boolean validate() {
		boolean validates = true;
		if (program.shardPid == null) {
			validates = false;
			extractionFailed("Validation failed: No shard Pid");
		}
		if (program.pbcoreProgramMetadata == null) {
			validates = false;
			extractionFailed("Validation failed: No PBCoreMetadata found");
		} else {
			if (program.pbcoreProgramMetadata.channel == null || program.pbcoreProgramMetadata.titel == null || program.pbcoreProgramMetadata.start == null || program.pbcoreProgramMetadata.end == null) {
				validates = false;
				extractionFailed("Validation failed: Incomplete PBCoreMetadata");
			}
		}
		return validates;
	}


	public boolean isExtractionSuccessful() {
		return !isExtractionFailed;
	}

	private boolean isExtractionFailed() {
		return isExtractionFailed;
	}

	public boolean isFilteredOut() {
		return isFilteredOut;
	}

	public void extractionFailed(String failStatus) {
		this.isExtractionFailed = true;
		this.statusMessages.add(failStatus);
	}

	public void filteredOut(String filterMessage) {
		this.isFilteredOut = true;
		this.statusMessages.add(filterMessage);
	}

	public boolean isReadyForExport() {
		return !isFilteredOut() && !isExtractionFailed();
	}

	public String getReasonForNotReadyForExport() {
		String reasons = "";
		int i = 1;
		for (String reason : statusMessages) {
			if (i>1) {
				reasons+="\n";
			}
			reasons+= "Reason " + i + ": " + reason;
		}
		return reasons;
	}
}
