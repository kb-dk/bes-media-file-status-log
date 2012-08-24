package dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.extraction.exception;


@SuppressWarnings("serial")
public class DOMSMetadataExtractionParseFilenameException extends DOMSMetadataExtractionParseException {

	public DOMSMetadataExtractionParseFilenameException(String msg) {
		super(msg);
	}

	public DOMSMetadataExtractionParseFilenameException(String msg, Throwable t) {
		super(msg, t);
	}

}
