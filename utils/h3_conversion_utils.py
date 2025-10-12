import h3
import json
import psycopg2
from psycopg2.extras import RealDictCursor


def load_config():
    """Load database configuration from app.json"""
    with open('app.json', 'r') as f:
        config = json.load(f)
    return config


def connect_to_database():
    """Connect to the database using credentials from app.json"""
    config = load_config()
    
    # Use Redshift connection details
    conn = psycopg2.connect(
        host=config['AWS_HOST'],
        port=config['AWS_PORT'],
        database=config['AWS_REDSHIFT_DATABASE'],
        user=config['AWS_USERNAME'],
        password=config['AWS_PASSWORD']
    )
    return conn


def convert_h3_resolution(h3_indexes_str, from_resolution=12, to_resolution=10):
    """
    Convert H3 indexes from one resolution to another.
    
    Args:
        h3_indexes_str: Comma-separated string of H3 integer indexes
        from_resolution: Source resolution (default: 12)
        to_resolution: Target resolution (default: 10)
    
    Returns:
        Comma-separated string of converted H3 integer indexes
    """
    # Handle None or empty values
    if h3_indexes_str is None or h3_indexes_str.strip() == '':
        return ''
    
    # Split the comma-separated string into individual H3 indexes
    h3_indexes = [int(idx.strip()) for idx in h3_indexes_str.split(',') if idx.strip()]
    
    converted_indexes = []
    
    for h3_int in h3_indexes:
        try:
            # Convert integer to H3 string
            h3_str = h3.int_to_str(h3_int)
            
            # Verify the resolution matches expected source resolution
            current_res = h3.get_resolution(h3_str)
            if current_res != from_resolution:
                print(f"Warning: H3 index {h3_int} has resolution {current_res}, expected {from_resolution}")
            
            # Convert to target resolution
            if to_resolution < current_res:
                # Convert to parent (lower resolution)
                converted_str = h3.cell_to_parent(h3_str, to_resolution)
            elif to_resolution > current_res:
                # Convert to children (higher resolution) - get first child
                children = h3.cell_to_children(h3_str, to_resolution)
                converted_str = children[0] if children else h3_str
            else:
                # Same resolution, no conversion needed
                converted_str = h3_str
            
            # Convert back to integer
            converted_int = h3.str_to_int(converted_str)
            converted_indexes.append(converted_int)
            
        except Exception as e:
            print(f"Error converting H3 index {h3_int}: {e}")
            # Keep original index if conversion fails
            converted_indexes.append(h3_int)
    
    return ','.join(map(str, converted_indexes))


def convert_h3_from_query(query, source_resolution, target_resolution, h3_column='h3_indexes'):
    """
    Simple function to convert H3 indexes from a database query.
    
    Args:
        query: SQL query string that returns rows with H3 indexes
        source_resolution: Source H3 resolution (e.g., 12)
        target_resolution: Target H3 resolution (e.g., 10)
        h3_column: Name of the column containing H3 indexes (default: 'h3_indexes')
    
    Returns:
        List of unique converted H3 indexes as integers
    """
    try:
        # Connect to database
        conn = connect_to_database()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Execute the query
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Set to store unique H3 indexes
        unique_h3_set = set()
        
        # Process each row
        for row in results:
            h3_indexes = row[h3_column]
            
            # Convert H3 indexes from source resolution to target resolution
            converted_h3 = convert_h3_resolution(h3_indexes, from_resolution=source_resolution, to_resolution=target_resolution)
            
            # Extract individual H3 indexes and add to set (automatically removes duplicates)
            if converted_h3:
                individual_h3s = converted_h3.split(',')
                for h3_str in individual_h3s:
                    if h3_str.strip():
                        unique_h3_set.add(int(h3_str.strip()))
        
        # Close connection
        cursor.close()
        conn.close()
        
        # Convert set to sorted list
        unique_h3_list = sorted(list(unique_h3_set))
        
        return unique_h3_list
        
    except Exception as e:
        print(f"Error: {e}")
        return []


