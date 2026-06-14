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
        # The CSV retains metadata rows at the top; the real column headers appear
        # as the first data row after Spark reads with header=True on the junk row.
        df = spark.read.csv(path_to_read, header=True)
        cols = []
        firstRow = df.first()
        for i in range(len(firstRow)):
            cols.append(firstRow[i])
        df = df.toDF(*cols)

        # Remove duplicate/repeated header rows generically.
        # Previously this was hardcoded to col('MERCHANT CODE') which broke for
        # files that don't carry that column (e.g. MAPS SFTP outward remittance).
        # Fix: use the first column — a duplicated header row will have its own
        # column name as its value, regardless of the file's schema.
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
