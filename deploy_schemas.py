# Databricks notebook source
#New Line added
# added from git
# added line from git now
import os
import re
from databricks import sql
from databricks.sql import Error as DatabricksError
import logging
from typing import List, Dict, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('schema_deployment.log')
    ]
)
logger = logging.getLogger(__name__)

class SchemaDeployer:
    def __init__(self, host: str, token: str, http_path: str = None):
        """Initialize the SchemaDeployer with Databricks connection details."""
        self.host = host
        self.token = token
        self.http_path = http_path or "/sql/1.0/warehouses/" + os.getenv("DATABRICKS_WAREHOUSE_ID", "")
        self.connection = None

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensure connection is closed."""
        self.close()

    def connect(self):
        """Establish connection to Databricks SQL endpoint."""
        try:
            self.connection = sql.connect(
                server_hostname=self.host,
                http_path=self.http_path,
                access_token=self.token
            )
            logger.info("Successfully connected to Databricks SQL endpoint")
        except Exception as e:
            logger.error(f"Failed to connect to Databricks: {str(e)}")
            raise

    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")

    def _ensure_catalog_exists(self, catalog: str):
        """Ensure that the specified catalog exists.
        
        This method first tries to create the catalog without a managed location.
        If that fails, it will try with a default location in the workspace's DBFS.
        """
        check_catalog_sql = f"""
        SELECT 1 
        FROM system.information_schema.catalogs 
        WHERE catalog_name = '{catalog}'
        """
        
        # First try: Create catalog without specifying a location
        create_catalog_sql = f"""
        CREATE CATALOG IF NOT EXISTS `{catalog}`
        """
        
        try:
            with self.connection.cursor() as cursor:
                # Check if catalog already exists
                cursor.execute(check_catalog_sql)
                if cursor.fetchone():
                    logger.info(f"Catalog {catalog} already exists")
                    return
                
                # Try to create catalog without location
                logger.info(f"Attempting to create catalog: {catalog}")
                try:
                    cursor.execute(create_catalog_sql)
                    logger.info(f"Successfully created catalog: {catalog}")
                    return
                except Exception as create_error:
                    logger.info(f"Standard catalog creation failed: {str(create_error)}")
                    
                # If we get here, the first attempt failed, try with a DBFS location
                try:
                    # Use a DBFS location that should be writable
                    dbfs_location = f"dbfs:/mnt/accelerated_operational_analytics/{catalog}"
                    create_with_location = f"""
                    CREATE CATALOG IF NOT EXISTS `{catalog}`
                    MANAGED LOCATION '{dbfs_location}'
                    """
                    logger.info(f"Attempting to create catalog with DBFS location: {dbfs_location}")
                    cursor.execute(create_with_location)
                    logger.info(f"Successfully created catalog with DBFS location: {catalog}")
                except Exception as dbfs_error:
                    # If DBFS location also fails, try one more time without any location
                    logger.warning(f"DBFS location creation failed: {str(dbfs_error)}")
                    logger.info("Falling back to basic catalog creation")
                    cursor.execute(create_catalog_sql)
                    logger.info(f"Successfully created basic catalog: {catalog}")
                    
        except Exception as e:
            error_msg = str(e).lower()
            if 'already exists' in error_msg:
                logger.info(f"Catalog {catalog} already exists")
            else:
                logger.error(f"Error creating catalog {catalog}: {str(e)}")
                raise

    def _ensure_schema_exists(self, catalog: str, schema: str):
        """Ensure that the specified schema exists in the catalog."""
        # First ensure the catalog exists
        self._ensure_catalog_exists(catalog)
        
        check_schema_sql = f"""
        SELECT 1 
        FROM system.information_schema.schemata 
        WHERE catalog_name = '{catalog}' AND schema_name = '{schema}'
        """
        
        create_schema_sql = f"""
        CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(check_schema_sql)
                if not cursor.fetchone():
                    logger.info(f"Creating schema: {catalog}.{schema}")
                    cursor.execute(create_schema_sql)
                    logger.info(f"Successfully created schema: {catalog}.{schema}")
        except Exception as e:
            logger.error(f"Error creating schema {catalog}.{schema}: {str(e)}")
            raise

    def _extract_schemas_from_sql(self, sql_content: str, catalog: str) -> set:
        """Extract schema names from SQL content."""
        # Look for patterns like `catalog`.schema or `catalog.schema`
        pattern = r'`' + re.escape(catalog) + r'`\.`?([a-zA-Z0-9_]+)`?'
        schemas = set(re.findall(pattern, sql_content, re.IGNORECASE))
        
        # Also look for unqualified schema references (e.g., just `schema`.table)
        unqualified_pattern = r'`?([a-zA-Z0-9_]+)`?\.'
        unqualified_schemas = set(re.findall(unqualified_pattern, sql_content, re.IGNORECASE))
        
        # Filter out any SQL keywords that might match the pattern
        sql_keywords = {'select', 'from', 'where', 'group', 'order', 'by', 'join', 'inner', 'left', 'right', 'outer'}
        unqualified_schemas = {s for s in unqualified_schemas if s.lower() not in sql_keywords}
        
        return schemas.union(unqualified_schemas)

    def execute_sql_file(self, file_path: str, catalog: str):
        """Execute SQL commands from a file."""
        try:
            with open(file_path, 'r') as file:
                sql_content = file.read()
                
                # Replace catalog placeholder if it exists in the SQL
                sql_content = sql_content.replace('${catalog}', catalog)
                
                # Handle catalog references in backticks (e.g., `dbx-prod-fin-catalog`.gold.table)
                sql_content = re.sub(r'`[^`]+`\.', f'`{catalog}`.', sql_content)
                
                # Extract and create schemas if they don't exist
                schemas = self._extract_schemas_from_sql(sql_content, catalog)
                for schema in schemas:
                    if schema.lower() not in ['information_schema', 'system']:  # Skip system schemas
                        self._ensure_schema_exists(catalog, schema)
                
                # Split SQL file into individual statements
                statements = self._split_sql_statements(sql_content)
                
                with self.connection.cursor() as cursor:
                    for stmt in statements:
                        if not stmt.strip():
                            continue
                            
                        logger.info(f"Executing: {stmt[:100]}..." if len(stmt) > 100 else f"Executing: {stmt}")
                        try:
                            cursor.execute(stmt)
                            logger.info("Statement executed successfully")
                        except DatabricksError as e:
                            logger.error(f"Error executing statement: {str(e)}")
                            # Continue with next statement even if one fails
                            continue
                            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
            raise

    def _split_sql_statements(self, sql_content: str) -> List[str]:
        """Split SQL content into individual statements."""
        # Remove single-line comments (--)
        sql_content = re.sub(r'--.*?$', '', sql_content, flags=re.MULTILINE)
        
        # Remove multi-line comments (/* */) but preserve TBLPROPERTIES and CREATE statements
        def remove_comments(match):
            content = match.group(0)
            if any(x in content.upper() for x in ['TBLPROPERTIES', 'CREATE ', 'SCHEMA ']):
                return content
            return ''
            
        sql_content = re.sub(r'/\*.*?\*/', remove_comments, sql_content, flags=re.DOTALL)
        
        # Normalize whitespace and remove empty lines
        sql_content = '\n'.join(line.strip() for line in sql_content.splitlines() if line.strip())
        
        # Split on semicolon, but not within string literals, dollar-quoted strings, or TBLPROPERTIES
        statements = []
        current = ""
        in_string = False
        in_tblproperties = False
        string_delimiter = None
        
        for i, char in enumerate(sql_content):
            # Handle TBLPROPERTIES block
            if sql_content[i:i+13].upper() == 'TBLPROPERTIES':
                in_tblproperties = True
            
            # Handle string literals
            if char in ("'", '"', '`') and not in_string and not in_tblproperties:
                in_string = True
                string_delimiter = char
                current += char
            elif char == string_delimiter and in_string and not in_tblproperties:
                in_string = False
                current += char
            # Handle semicolon - only split if not in string or TBLPROPERTIES
            elif char == ';' and not in_string and not in_tblproperties:
                stmt = current.strip()
                if stmt:  # Only add non-empty statements
                    statements.append(stmt)
                current = ""
            # Handle end of TBLPROPERTIES block
            elif char == ')' and in_tblproperties:
                current += char
                in_tblproperties = False
                # Add the complete TBLPROPERTIES statement
                stmt = current.strip()
                if stmt:
                    statements.append(stmt)
                current = ""
            else:
                current += char
        
        # Add any remaining content as the last statement
        if current.strip():
            statements.append(current.strip())
        
        # Filter out empty statements and ensure proper formatting
        return [s + ';' if not s.endswith(';') else s for s in statements if s.strip()]

def get_sql_files(schema_dir: str) -> List[str]:
    """Get list of SQL files in the specified directory, sorted by dependency order."""
    # Define the order of execution based on file name patterns
    priority_order = [
        'dim_',  # Dimension tables first
        'fact_', # Then fact tables
        'silver_', # Then silver layer
        'gold_'   # Finally gold layer
    ]
    
    # Get all SQL files
    files = [f for f in os.listdir(schema_dir) if f.endswith('.sql')]
    
    # Sort files based on priority order
    def get_priority(filename):
        for i, pattern in enumerate(priority_order):
            if pattern.lower() in filename.lower():
                return i
        return len(priority_order)  # Default to lowest priority
    
    return sorted(files, key=get_priority)

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Deploy SQL schemas to Databricks')
    parser.add_argument('--host', required=True, help='Databricks workspace URL')
    parser.add_argument('--token', required=True, help='Databricks access token')
    parser.add_argument('--catalog', required=True, help='Catalog name')
    parser.add_argument('--schema-dir', default='schema', help='Directory containing SQL schema files')
    parser.add_argument('--http-path', help='Databricks SQL warehouse HTTP path')
    
    args = parser.parse_args()
    
    try:
        # Get list of SQL files in dependency order
        sql_files = get_sql_files(args.schema_dir)
        
        if not sql_files:
            logger.warning(f"No SQL files found in {args.schema_dir}")
            return
            
        logger.info(f"Found {len(sql_files)} SQL files to execute")
        
        # Execute each SQL file
        with SchemaDeployer(args.host, args.token, args.http_path) as deployer:
            for sql_file in sql_files:
                file_path = os.path.join(args.schema_dir, sql_file)
                logger.info(f"Processing {sql_file}...")
                deployer.execute_sql_file(file_path, args.catalog)
                
        logger.info("Schema deployment completed successfully")
        
    except Exception as e:
        logger.error(f"Schema deployment failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
