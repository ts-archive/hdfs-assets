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
`json_lines` format.

### Parameters

| Name | Description | Default | Required |
| ---- | ----------- | ------- | -------- |
| user | Specifies the user to connect to HDFS with | hdfs | N |
| path | Location of files to process | -- | Y |
| size | Specifies slice size in bytes | 100000 | N |
| format | Format of the target file. Currently only supports "jsonLines" | jsonLines | N |
| connection | Specifies the HDFS connector to use | default | N |

In the event a slice ends in the middle of a record, the reader will read ahead in the file in order
to make sure the last slice is completely read from the file. Additionally, if a slice begins with
a partial record, that will be dropped before trying to process the records.

# Chunk Formatting and Settings
The logic for determining file offsets, the need for reading extra margin, and formatting the data
has been externalized for use with other readers. The `chunk_settings` module interacts with the
processor using the `chunkOptions` object, which contains the settings needed for reading any given
chunk. The `chunkOptions` object has the following format:  

```
{
    path: '/some/path/to/data',
    needMargin: true,
    delimiter: '\n',
    data: 'some set of data read\nfrom a file',
    options: {
        offset: 25,
        length: 33
    },
    marginOptions: {
        offset: 58,
        length: 10
    }
}
```

The settings in this object are determined automatically by the settings in the opConfig
and the details about the slice. At the end of the reader, `chunk_settings.cleanData` is used to
break the raw data into records and return those in an array. The array of records is finally passed
through the `chunk_formatter` module to format the records based on what is specified in the
opConfig.
