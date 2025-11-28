import duckdb
import os
import json
from datetime import datetime
from pathlib import Path
from typing import Union, List
from sources.tools.tools import Tools

TOKEN_LIMIT = 5000

def _serialize_datetime(obj):
    """Convert datetime objects to ISO format strings for JSON serialization"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: _serialize_datetime(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [_serialize_datetime(item) for item in obj]
    else:
        return obj

def _estimate_token_count(text: str) -> int:
    """Estimate token count using character-based approximation."""
    average_chars_per_token = 3
    return (len(text) + average_chars_per_token - 1) // average_chars_per_token

def _enforce_token_limit(payload: str, context: str) -> str:
    """Ensure payload stays within the token budget before returning"""
    token_estimate = _estimate_token_count(payload)
    if token_estimate <= TOKEN_LIMIT:
        return payload

    current_size = len(json.loads(payload)) if payload.startswith("[") else None
    suggested_limit = None
    if current_size:
        ratio = TOKEN_LIMIT / token_estimate
        suggested_limit = max(1, int(current_size * ratio * 0.8))  # 80% safety margin

    suggestion_parts = [
        "The query result is too large. Please adjust your query:",
        "  • Reduce the LIMIT value" + (f" (try LIMIT {suggested_limit})" if suggested_limit else ""),
        "  • Filter rows with WHERE clauses to reduce result size",
        "  • Select only necessary columns instead of SELECT *",
        "  • Use aggregation (COUNT, SUM, AVG) instead of retrieving raw rows",
    ]

    warning = {
        "error": "Result exceeds token budget",
        "context": context,
        "estimated_tokens": token_estimate,
        "token_limit": TOKEN_LIMIT,
        "rows_returned": current_size,
        "suggested_limit": suggested_limit,
        "suggestion": "\n".join(suggestion_parts),
    }
    return json.dumps(warning, ensure_ascii=False, indent=2)

class ListTablesInDirectory(Tools):
    def __init__(self):
        super().__init__()
        self.tag = "list_tables_in_directory"
        self.name = "List Tables In Directory"
        self.description = "List all parquet files in the current directory or a specified directory. Usage: ```list_tables_in_directory\ndirectory=.\n```"

    def execute(self, blocks, safety=False):
        output = ""
        for block in blocks:
            args = {}
            for line in block.strip().split('\n'):
                if '=' in line:
                    key, value = line.split('=', 1)
                    args[key.strip()] = value.strip()
            
            path = args.get('directory', '.')
            if path == '.':
                path = self.get_work_dir()
            
            try:
                if not os.path.exists(path):
                     output += f"Error: Directory '{path}' does not exist.\n"
                     continue
                
                files = [f for f in os.listdir(path) if f.endswith('.parquet')]
                if not files:
                    output += f"No parquet files found in {path}.\n"
                else:
                    output += f"Parquet files in {path}:\n" + "\n".join(files) + "\n"
            except Exception as e:
                output += f"Error listing files in {path}: {str(e)}\n"
        return output

    def execution_failure_check(self, output):
        return "Error" in output

    def interpreter_feedback(self, output):
        return output

class GetSchema(Tools):
    def __init__(self):
        super().__init__()
        self.tag = "get_schema"
        self.name = "Get Schema"
        self.description = "Get the schema of a parquet file. Usage: ```get_schema\nfile_path=logs.parquet\n```"

    def execute(self, blocks, safety=False):
        output = ""
        for block in blocks:
            args = {}
            for line in block.strip().split('\n'):
                if '=' in line:
                    key, value = line.split('=', 1)
                    args[key.strip()] = value.strip()
            
            file_path = args.get('file_path')
            if not file_path:
                output += "Error: file_path argument is required.\n"
                continue

            # Resolve path relative to work_dir if not absolute and not found in CWD
            if not os.path.isabs(file_path) and not os.path.exists(file_path):
                work_dir_path = os.path.join(self.get_work_dir(), file_path)
                if os.path.exists(work_dir_path):
                    file_path = work_dir_path

            try:
                con = duckdb.connect()
                query = f"DESCRIBE SELECT * FROM '{file_path}'"
                df = con.execute(query).df()
                output += f"Schema for {file_path}:\n{df.to_string()}\n"
            except Exception as e:
                output += f"Error inspecting schema for {file_path}: {str(e)}\n"
        return output

    def execution_failure_check(self, output):
        return "Error" in output

    def interpreter_feedback(self, output):
        return output

class QueryParquetFiles(Tools):
    def __init__(self):
        super().__init__()
        self.tag = "query_parquet_files"
        self.name = "Query Parquet Files"
        self.description = "Execute SQL queries on parquet files using DuckDB. Usage: ```query_parquet_files\nparquet_files=['file1.parquet', 'file2.parquet']\nquery=SELECT * FROM file1 JOIN file2 ...\nlimit=10\n```"

    def execute(self, blocks, safety=False):
        output = ""
        for block in blocks:
            # Robust parsing for multi-line query
            args = {}
            lines = block.strip().split('\n')
            current_key = None
            current_value = []
            
            for line in lines:
                if '=' in line and (line.startswith('parquet_files=') or line.startswith('query=') or line.startswith('limit=')):
                     if current_key:
                         args[current_key] = '\n'.join(current_value).strip()
                     parts = line.split('=', 1)
                     current_key = parts[0].strip()
                     current_value = [parts[1].strip()]
                elif current_key:
                    current_value.append(line)
            
            if current_key:
                args[current_key] = '\n'.join(current_value).strip()

            parquet_files_str = args.get('parquet_files', "[]")
            try:
                parquet_files = eval(parquet_files_str) if parquet_files_str.startswith('[') else [parquet_files_str]
                if isinstance(parquet_files, str): parquet_files = [parquet_files]
            except:
                parquet_files = [parquet_files_str]

            query = args.get('query', '')
            limit = int(args.get('limit', 10))

            if not query:
                output += "Error: query argument is required.\n"
                continue

            try:
                conn = duckdb.connect(":memory:")
                table_names = set()
                
                for file_path in parquet_files:
                    # Resolve path relative to work_dir if not absolute and not found in CWD
                    if not os.path.isabs(file_path) and not os.path.exists(file_path):
                        work_dir_path = os.path.join(self.get_work_dir(), file_path)
                        if os.path.exists(work_dir_path):
                            file_path = work_dir_path
                    
                    if not os.path.exists(file_path):
                         output += f"Error: Parquet file not found: {file_path}\n"
                         continue

                    base_name = Path(file_path).stem
                    table_name = base_name
                    counter = 1
                    while table_name in table_names:
                        table_name = f"{base_name}_{counter}"
                        counter += 1
                    table_names.add(table_name)
                    conn.execute(f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet('{file_path}')")

                result = conn.execute(query).fetchall()
                columns = [desc[0] for desc in conn.description]
                rows = [dict(zip(columns, row)) for row in result]
                serialized_rows = _serialize_datetime(rows)

                if len(serialized_rows) > limit:
                    serialized_rows = serialized_rows[:limit]

                result_json = json.dumps(serialized_rows, ensure_ascii=False, indent=2)
                output += _enforce_token_limit(result_json, "query_parquet_files") + "\n"
                conn.close()

            except Exception as e:
                output += f"Error executing query: {str(e)}\n"
        
        return output

    def execution_failure_check(self, output):
        return "Error" in output

    def interpreter_feedback(self, output):
        return output
