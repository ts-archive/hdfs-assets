# hdfs-assets

This is a bundle of processors for reading from and writing to HDFS.

## hdfs_append

This is a processor for appending data to files in HDFS.

### Parameters

| Name | Description | Default | Required |
| ---- | ----------- | ------- | -------- |
| connection | Specifies the HDFS connector to use | default | N |
| log_data_on_error | Specifies whether or not to log the slice data if there is a processing error | false | N |
| max_write_errors | Determines how many new files a worker can create if there is an append error | 100 | N |

There is a known append error in HDFS where a block can be corrupted if a replica is relocated
during the write process. This processor can be configured to work around that issue by creating a
new file when a worker encounters this problem.


## hdfs_file_chunker

This is a processor for batching records into HDFS-friendly chunks for use with the `hdfs_append`
processor.

### Parameters

| Name | Description | Default | Required |
| ---- | ----------- | ------- | -------- |
| timeseries | Set to an interval to have directories named using a date field from the data records | null | N |
| date_field | Which field in each data record contains the date to use for timeseries. Only useful if "timeseries" is also specified. | date | N |
| directory | Path to use when generating the file name | / | N |
| filename | Not recommended for HDFS. If not specified a filename will be automatically chosen to reduce the occurrence of concurrency issues when writing to HDFS. | '' | N |
| chunk_size | Specifies how many records to batch into a single chunk | 50000 | N |

## hdfs_reader

This is a processor for reading data out of HDFS file. Currently only supports data stored in
`json_lines` format and is only intended for use as a `once` job.

### Parameters

| Name | Description | Default | Required |
| ---- | ----------- | ------- | -------- |
| user | Specifies the user to connect to HDFS with | hdfs | N |
| path | Location of files to process | -- | Y |
| size | Specifies slice size in bytes | 100000 | N |
| format | Format of the target file. Currently only supports "json" | json | N |
| connection | Specifies the HDFS connector to use | default | N |

In the event a slice ends in the middle of a record, the reader will read ahead in the file in order
to make sure the last slice is completely read from the file. Additionally, if a slice begins with
a partial record, that will be dropped before trying to process the records.
