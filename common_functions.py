from pyspark.sql.functions import col


def file_reader_function(file_type, path_to_read, file_config, table_id):
    print(f"file_type:{file_type}")
    print(f"path_to_read:{path_to_read}")

    if file_type == 'parquet':
        df = spark.read.parquet(path_to_read)

    elif file_type == "xlsx":
        df = spark.read.csv(path_to_read, header=True)
        cols = []
        firstRow = df.first()
        for i in range(len(firstRow)):
            val = firstRow[i]
            # Replace None column values with a placeholder to avoid NOT_LIST_OF_STR error
            cols.append(str(val) if val is not None else f'_col{i}')
        df = df.toDF(*cols)
        df = df.filter(col('MERCHANT CODE') != 'MERCHANT CODE')

    elif file_type == 'zip' or file_type == 'xls':
        header = file_config['header']
        inferSchema = file_config['inferSchema']
        quote = file_config['quote']
        multiline = file_config['multiline']
        escape = file_config['escape']
        sep = file_config['sep']
        df = spark.read.csv(path_to_read, header=header, inferSchema=inferSchema)

    elif file_type == 'csv':
        header = file_config['header']
        inferSchema = file_config['inferSchema']
        mergeSchema = file_config['mergeSchema'] if 'mergeSchema' in file_config else None
        quote = file_config['quote']

        df = spark.read.csv(path_to_read, header=header, inferSchema=inferSchema)

    else:
        raise ValueError(f"Unsupported data type: {file_type}")

    return df
