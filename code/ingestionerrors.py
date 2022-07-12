
errorMap={
"File has no data, only headers":"the file is empty, it has no data",
"java.lang.NullPointerException":"the file is no longer present",
"Fail validateHeaders! ParsedFileRecord is missing":"because of GSSD headers validation issue",
"Delimiter validator failed, unable to detect delimiter":"the delimiter validator has failed",
"OverSizedFileException: The file has too many columns":"the file has more than 2000 columns",
"Invalid ingestion request step: CANCELLED. Only VALIDATION or FILE_PROCESSOR are supported com.liveramp.dataflow.validation.ValidationFn.runValidator":"of invalid type of ingestion",
"Too many unique segments":"there are too many unique segments, max segment count is250",
"NumberFormatException: multiple points":"of number format issue",
"ParsedFileRecord is missing":"the parsedFileRecord is missing",
"Record doesn't contain all 4 GSSD metadata headers":"as some metadata headers are missing",
"batch results and batch inputs are not the same size!":"batch results and batch inputs are not the same size!"
}

toBeSkipped=["http.conn.ConnectTimeoutException:"]