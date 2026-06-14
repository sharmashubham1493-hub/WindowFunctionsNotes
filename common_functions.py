from pyspark.sql.functions import col


def special_char_remover(df):
    """Strips whitespace and lowercases all column names."""
    new_cols = [c.strip().lower() for c in df.columns]
    return df.toDF(*new_cols)


def file_reader_function(file_type, path_to_read, file_config, table_id):
    print(f"file_type:{file_type}")
    print(f"path_to_read:{path_to_read}")

    if file_type == 'parquet':
        df = spark.read.parquet(path_to_read)

    elif file_type == 'xlsx':
        # xlsx files are pre-converted to CSV by ADF before landing in ADLS.
        # file_config['header_row'] (1-based) tells us which row holds the real
        # column headers.  Defaults to 2 for backward-compatibility (old files
        # had one junk metadata row at the top, so row 2 was the real header).
        header_row = file_config.get('header_row', 8)  # 1-based row number

        if header_row <= 2:
            # Legacy path: read with Spark header=True (row 1 becomes column
            # names), then promote the first data row (row 2) to be the header.
            df = spark.read.csv(path_to_read, header=True)
            cols = [v for v in df.first()]
            df = df.toDF(*cols)
        else:
            # General path: header is deeper in the file (e.g. row 8 for MAPS
            # SFTP files that have 7 rows of metadata before the column names).
            # Read without any header, zip with sequential indices, pluck the
            # header row, then keep only the rows that follow it.
            df_raw = spark.read.csv(path_to_read, header=False)
            indexed = df_raw.rdd.zipWithIndex()
            hdr_index = header_row - 1  # convert to 0-based
            header_values = indexed.filter(lambda x: x[1] == hdr_index).first()[0]
            cols = [
                str(v) if v is not None else f"_c{i}"
                for i, v in enumerate(header_values)
            ]
            data_rdd = indexed.filter(lambda x: x[1] > hdr_index).map(lambda x: x[0])
            df = spark.createDataFrame(data_rdd, df_raw.schema)
            df = df.toDF(*cols)

        # Remove duplicate/repeated header rows generically.
        # A duplicated header row has the column name as its own value regardless
        # of the file's schema, so using the first column works for any xlsx file.
        first_col = df.columns[0]
        df = df.filter(col(first_col).isNotNull() & (col(first_col) != first_col))

    elif file_type == 'zip' or file_type == 'xls':
        header = file_config['header']
        inferSchema = file_config['inferSchema']
        quote = file_config['quote']
        multiline = file_config['multiline']
        escape = file_config['escape']
        sep = file_config['sep']
        df = spark.read.csv(
            path_to_read,
            header=header,
            inferSchema=inferSchema,
            multiline=multiline,
            sep=sep,
        )

    elif file_type == 'csv':
        header = file_config['header']
        inferSchema = file_config['inferSchema']
        quote = file_config['quote']
        multiline = file_config['multiline']
        escape = file_config['escape']
        sep = file_config['sep']
        mergeSchema = file_config.get('mergeSchema', False)
        df = spark.read.csv(
            path_to_read,
            header=header,
            inferSchema=inferSchema,
            quote=quote,
            multiline=multiline,
            escape=escape,
            sep=sep,
        )

    else:
        print(f"File Type Not Found in function. Please add the FileType: {file_type}")
        return None

    # Apply special character / whitespace cleanup to column names.
    # Bug fix: result must be assigned back to df (was previously assigned to `of`).
    df = special_char_remover(df)
    return df
