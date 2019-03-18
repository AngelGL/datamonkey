# PROCESSING
BASE_API_URL = "https://api.data-monkey.com/v1/"
CHUNKSIZE = 1000000

# PANDAS/PYTHON/FILE TYPE MAPPINGS
PANDAS_TYPE_MAP = {"STRING": 'O', "INT": "int64", "FLOAT": "float64", "BOOLEAN": "bool", "DATE": "datetime64[ns]", "DATETIME": "datetime64[ns]"}
PYTHON_TYPE_MAP = {"STRING": str, "INT": int, "FLOAT": float, "BOOLEAN": bool, "DATE": "O", "DATETIME": "o"}
INFERRED_DATA_TYPE_MAP = {'object': "STRING", "int64": "INT", "float64": "FLOAT", "bool": "BOOLEAN", 'datetime64[ns]': 'DATE'}
INFERRED_FILE_TYPE_MAP = {'.xls': "EXCEL", ".xlsx": "EXCEL", ".csv": "CSV", ".json": "JSON", ".txt": "FWF"}
DMK_TYPE_MAP = {"STRING": "string", "INT": "number", "FLOAT": "decimal", "BOOLEAN": "boolean", "DATE": "date", "DATETIME": "datetime"}
