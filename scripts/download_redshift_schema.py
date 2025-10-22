#!/usr/bin/env python3
"""
Redshift Schema Downloader
Downloads schema information from Redshift tables used in the property system
"""

import json
import logging
import os
import sys
from datetime import datetime
from psycopg2.extras import RealDictCursor

# Add parent directory to path to import utils
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

# Now import after adding to path
from utils.dbUtils import RedshiftDatabase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RedshiftSchemaDownloader:
    def __init__(self):
        self.redshift_db = RedshiftDatabase()
        self.schema_data = {
            "download_timestamp": datetime.now().isoformat(),
            "tables": {},
            "relationships": [],
            "sample_data": {}
        }
    
    def download_schema(self):
        """Download complete schema information"""
        logger.info("Starting Redshift schema download...")
        
        try:
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            # Define tables to analyze (with correct schema names)
            tables_to_analyze = [
                "blackprint_db_prd.data_product.v_qro",
                "blackprint_db_prd.presentation.dim_market_data_combined", 
                "blackprint_db_prd.presentation.dim_municipality_qro",
                "blackprint_db_prd.data_product.v_parcel_v3",
                "blackprint_db_prd.presentation.dataset_mobility_data_v2",
                "blackprint_db_prd.staging.stg_demographic_socioeconomic_qro",
                "blackprint_db_prd.presentation.dim_pois_qro"
            ]
            
            for table in tables_to_analyze:
                logger.info(f"Analyzing table: {table}")
                self._analyze_table(cursor, table)
            
            # Get relationships between tables
            self._get_table_relationships(cursor)
            
            cursor.close()
            connection.close()
            
            # Save schema to file
            self._save_schema_to_file()
            
            logger.info("Schema download completed successfully!")
            return self.schema_data
            
        except Exception as e:
            logger.error(f"Error downloading schema: {str(e)}")
            raise
    
    def _analyze_table(self, cursor, table_name):
        """Analyze a specific table"""
        try:
            # Get column information
            column_query = f"""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns 
            WHERE table_schema = '{table_name.split('.')[0]}' 
            AND table_name = '{table_name.split('.')[1]}'
            ORDER BY ordinal_position;
            """
            
            cursor.execute(column_query)
            columns = cursor.fetchall()
            
            # Get sample data (first 5 rows)
            sample_query = f"SELECT * FROM {table_name} LIMIT 5;"
            cursor.execute(sample_query)
            sample_data = cursor.fetchall()
            
            # Get table statistics
            stats_query = f"""
            SELECT COUNT(*) as row_count
            FROM {table_name};
            """
            cursor.execute(stats_query)
            stats = cursor.fetchone()
            
            # Store table information
            self.schema_data["tables"][table_name] = {
                "columns": [dict(col) for col in columns],
                "row_count": stats['row_count'] if stats else 0,
                "sample_data": [dict(row) for row in sample_data],
                "description": self._get_table_description(table_name)
            }
            
            logger.info(f"[OK] Analyzed {table_name}: {len(columns)} columns, {stats['row_count'] if stats else 0} rows")
            
        except Exception as e:
            logger.warning(f"Could not analyze table {table_name}: {str(e)}")
            self.schema_data["tables"][table_name] = {
                "error": str(e),
                "columns": [],
                "row_count": 0,
                "sample_data": []
            }
    
    def _get_table_description(self, table_name):
        """Get description for table based on its purpose"""
        descriptions = {
            "presentation.dim_property_qro": "Queretaro property dimension table with basic property information",
            "presentation.dim_market_data_combined": "Combined market data for properties including prices and types",
            "presentation.dim_municipality_qro": "Queretaro municipality and location data",
            "data_product.v_property_mexico": "Mexico City property view with comprehensive property data",
            "data_product.v_demographic_mexico": "Mexico City demographic data",
            "data_product.v_traffic_mexico": "Mexico City traffic and mobility data",
            "data_product.v_municipality": "Mexico City municipality and location data"
        }
        return descriptions.get(table_name, "Property-related table")
    
    def _get_table_relationships(self, cursor):
        """Get relationships between tables"""
        try:
            # Get foreign key relationships
            fk_query = """
            SELECT 
                tc.table_schema || '.' || tc.table_name as source_table,
                kcu.column_name as source_column,
                ccu.table_schema || '.' || ccu.table_name as target_table,
                ccu.column_name as target_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage ccu 
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema IN ('presentation', 'data_product');
            """
            
            cursor.execute(fk_query)
            relationships = cursor.fetchall()
            
            self.schema_data["relationships"] = [dict(rel) for rel in relationships]
            
            logger.info(f"[OK] Found {len(relationships)} table relationships")
            
        except Exception as e:
            logger.warning(f"Could not get table relationships: {str(e)}")
            self.schema_data["relationships"] = []
    
    def _save_schema_to_file(self):
        """Save schema data to JSON file"""
        schema_file = "redshift_schema.json"
        
        with open(schema_file, 'w', encoding='utf-8') as f:
            json.dump(self.schema_data, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"Schema saved to: {schema_file}")
        
        # Also create a simplified version for MCP
        self._create_mcp_schema_file()
    
    def _create_mcp_schema_file(self):
        """Create simplified schema file for MCP usage"""
        mcp_schema = {
            "description": "Redshift schema for property search MCP",
            "tables": {}
        }
        
        for table_name, table_info in self.schema_data["tables"].items():
            if "error" not in table_info:
                # Create simplified column information
                columns = {}
                for col in table_info["columns"]:
                    columns[col["column_name"]] = {
                        "type": col["data_type"],
                        "nullable": col["is_nullable"] == "YES",
                        "description": self._get_column_description(table_name, col["column_name"])
                    }
                
                mcp_schema["tables"][table_name] = {
                    "description": table_info["description"],
                    "columns": columns,
                    "row_count": table_info["row_count"],
                    "sample_values": self._extract_sample_values(table_info["sample_data"])
                }
        
        mcp_schema_file = "mcp_schema.json"
        with open(mcp_schema_file, 'w', encoding='utf-8') as f:
            json.dump(mcp_schema, f, indent=2, ensure_ascii=False)
        
        logger.info(f"MCP schema saved to: {mcp_schema_file}")
    
    def _get_column_description(self, table_name, column_name):
        """Get description for column based on name and table"""
        descriptions = {
            "fid": "Property unique identifier",
            "lat": "Latitude coordinate",
            "lng": "Longitude coordinate", 
            "nom_mun": "Municipality name",
            "nom_loc": "Location/neighborhood name",
            "is_on_market": "Property availability status",
            "property_type": "Type of property (apartment, house, commercial)",
            "operation_type": "Operation type (renta, venta)",
            "buy_price_clean": "Clean buy price",
            "rent_price_clean": "Clean rent price",
            "property_dimension_clean": "Property size/dimension",
            "municipality_nm": "Municipality name",
            "neighborhood": "Neighborhood name",
            "zip_code": "Postal code",
            "total_surface_area": "Total surface area",
            "total_construction_area": "Total construction area"
        }
        return descriptions.get(column_name, f"Column: {column_name}")
    
    def _extract_sample_values(self, sample_data):
        """Extract sample values for each column"""
        if not sample_data:
            return {}
        
        sample_values = {}
        for row in sample_data:
            for key, value in row.items():
                if key not in sample_values:
                    sample_values[key] = []
                if value is not None and value not in sample_values[key]:
                    sample_values[key].append(str(value))
                    if len(sample_values[key]) >= 5:  # Limit to 5 sample values
                        break
        
        return sample_values

def main():
    """Main function to download schema"""
    try:
        downloader = RedshiftSchemaDownloader()
        schema_data = downloader.download_schema()
        
        print("\n" + "="*50)
        print("SCHEMA DOWNLOAD SUMMARY")
        print("="*50)
        
        for table_name, table_info in schema_data["tables"].items():
            if "error" not in table_info:
                print(f"[OK] {table_name}: {len(table_info['columns'])} columns, {table_info['row_count']} rows")
            else:
                print(f"[ERROR] {table_name}: Error - {table_info['error']}")
        
        print(f"\n[OK] Found {len(schema_data['relationships'])} table relationships")
        print(f"[OK] Schema files created: redshift_schema.json, mcp_schema.json")
        
    except Exception as e:
        logger.error(f"Schema download failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
