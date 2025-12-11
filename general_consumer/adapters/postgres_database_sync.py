from ..ports.database_sync import DatabaseSync
from datetime import datetime, date
from typing import Any, Dict, Optional
import json

from sqlalchemy import (
    create_engine, MetaData, Table, Column, String, Integer, Float,
    Boolean, inspect, select, exists, func, insert, update, Date, DateTime
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Engine

import traceback


class PostgresDatabaseSync(DatabaseSync):
    """PostgreSQL implementation of DatabaseSync using SQLAlchemy."""
    
    def __init__(self):
        """Initialize instance variables."""
        self.engine: Optional[Engine] = None
        self.metadata: Optional[MetaData] = None
    
    def log_in(
        self, 
        host: str, 
        port: int, 
        user: str, 
        password: str, 
        database_name: str, 
        any_other_credential: Optional[Dict[str, Any]] = None
    ) -> tuple[Engine, MetaData]:
        """
        Establish connection to PostgreSQL database.
        
        Args:
            host: Database host address
            port: Database port
            user: Database username
            password: Database password
            database_name: Name of the database
            any_other_credential: Optional additional credentials
            
        Returns:
            Tuple of (engine, metadata)
            
        Raises:
            Exception: If connection fails
        """
        try:
            database_url = f"postgresql://{user}:{password}@{host}:{port}/{database_name}"
            self.engine = create_engine(database_url)
            self.metadata = MetaData()
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(select(1))
                
            return self.engine, self.metadata
        except Exception as e:
            raise Exception(f"Failed to connect to database: {str(e)}") from e

    def map_datatypes(self, fields: Dict[str, str]) -> Dict[str, type]:
        """
        Map custom field types to SQLAlchemy types.
        
        Args:
            fields: Dictionary mapping field names to their type strings
            
        Returns:
            Dictionary mapping field names to SQLAlchemy column types
        """
        SQLA_TYPE_MAP = {
            "char": String,
            "text": String,
            "html": String,
            "selection": String,
            "binary": String,
            "many2one": String,
            "one2many": String,
            "many2many": String,
            "boolean": Boolean,
            "integer": Integer,
            "float": Float,
            "monetary": Float,
            "date": Date,
            "datetime": DateTime,
        }

        sql_fields = {}
        for field_name, field_type in fields.items():
            # Skip internal fields
            if field_name == 'odoo_internal_id':
                continue
            # Map to SQLAlchemy type, default to String if unknown
            sql_fields[field_name] = SQLA_TYPE_MAP.get(field_type, String)

        return sql_fields

    def create_table_if_not_exists(self, table_name: str, fields: Dict[str, type]) -> bool:
        """
        Create a table if it doesn't exist.
        
        Args:
            table_name: Name of the table to create
            fields: Dictionary mapping field names to SQLAlchemy types
            
        Returns:
            True if successful
            
        Raises:
            Exception: If engine or metadata not initialized
        """
        if self.engine is None or self.metadata is None:
            raise Exception("Database not connected. Call log_in() first.")
        
        # Check if table already exists
        if table_name in self.metadata.tables:
            return True
        
        # Create table with standard columns plus dynamic fields
        """
        table = Table(
            table_name, 
            self.metadata,
            Column('id', Integer, primary_key=True),           
            Column('operations_history', JSONB, nullable=True),
            Column('archived', Boolean, nullable=False, server_default='false'),
            *[Column(field, dtype, nullable=True) for field, dtype in fields.items()]
        )
        
        # Create table in database
        self.metadata.create_all(self.engine)
        """
        return True

    def exists_by_id(self, table_name: str, id: int) -> bool:
        """
        Check if a record exists by ID.
        
        Args:
            table_name: Name of the table
            id: Record ID to check
            
        Returns:
            True if record exists, False otherwise
        """
        if self.engine is None or self.metadata is None:
            raise Exception("Database not connected. Call log_in() first.")
        
        # Load table if not in metadata
        if table_name not in self.metadata.tables:
            table = Table(table_name, self.metadata, autoload_with=self.engine)
        else:
            table = self.metadata.tables[table_name]

        stmt = select(exists().where(table.c.id == id))

        with self.engine.connect() as conn:
            result = conn.scalar(stmt)
            return result if result is not None else False

    @staticmethod
    def _parse_iso(value: Any) -> Any:
        """
        Parse ISO format strings to datetime/date objects.
        
        Args:
            value: Value to parse (typically a string)
            
        Returns:
            Parsed datetime/date object, or original value if parsing fails
        """
        if not isinstance(value, str):
            return value
        
        # Try parsing as datetime
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            pass
        
        # Try parsing as date
        try:
            return date.fromisoformat(value)
        except ValueError:
            pass
        
        return value

    @staticmethod
    def _prepare_record_for_insert(raw_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare a raw dictionary for database insertion.
        
        Args:
            raw_dict: Raw record data from message
            
        Returns:
            Formatted record ready for insertion
        """
        special_fields = ['odoo_internal_id', 'operation', 'by', 'timestamp']
        
        # Extract metadata for operations_history
        metadata = {
            key: PostgresDatabaseSync._parse_iso(raw_dict[key]) 
            for key in special_fields 
            if key in raw_dict
        }

        operations_history = {"history": [metadata]}

        # Initialize record with id and operations_history
        record = {
            "id": raw_dict.get("odoo_internal_id"),
            "operations_history": operations_history
        }

        # Add remaining fields, parsing ISO strings
        for key, value in raw_dict.items():
            if key not in special_fields and key != 'odoo_internal_id':
                record[key] = PostgresDatabaseSync._parse_iso(value)

        return record

    def create_record(self, table_name: str, data: Dict[str, Any]) -> bool:
        """
        Create a new record in the table.
        
        Args:
            table_name: Name of the table
            data: Record data to insert
            
        Returns:
            True if successful
        """
        if self.engine is None or self.metadata is None:
            raise Exception("Database not connected. Call log_in() first.")
        
        # Load table if not in metadata
        if table_name not in self.metadata.tables:
            table = Table(table_name, self.metadata, autoload_with=self.engine)
        else:
            table = self.metadata.tables[table_name]

        prepared_data = self._prepare_record_for_insert(data)

        with self.engine.begin() as conn:
            conn.execute(insert(table).values(prepared_data))
        
        return True

    def update_record(self, table_name: str, id: int, data: Dict[str, Any]) -> bool:
        """
        Update an existing record.
        
        Args:
            table_name: Name of the table
            id: Record ID to update
            data: Updated record data
            
        Returns:
            True if successful
        """
        if self.engine is None or self.metadata is None:
            raise Exception("Database not connected. Call log_in() first.")
        
        # Load table if not in metadata
        if table_name not in self.metadata.tables:
            table = Table(table_name, self.metadata, autoload_with=self.engine)
        else:
            table = self.metadata.tables[table_name]

        # Extract operation history metadata
        history_keys = ['operation', 'by', 'timestamp']
        operation_info = {
            k: self._parse_iso(data.pop(k)) 
            for k in history_keys 
            if k in data
        }

        # Build update dict with only existing columns
        record_update = {}
        for key, value in data.items():
            if key in table.c and key != 'id':  # Don't update ID
                record_update[key] = self._parse_iso(value)

        # Append to operations history if column exists
        if 'operations_history' in table.c and operation_info:
            record_update['operations_history'] = func.jsonb_set(
                table.c.operations_history,
                '{history}',
                (table.c.operations_history['history'] + [operation_info]).cast(JSONB)
            )

        stmt = update(table).where(table.c.id == id).values(**record_update)

        with self.engine.begin() as conn:
            conn.execute(stmt)
        
        return True

    def set_archived_flag_record(
        self, 
        table_name: str, 
        id: int, 
        by: str = 'system', 
        timestamp: Optional[str] = None
    ) -> bool:
        """
        Set the archived flag on a record.
        
        Args:
            table_name: Name of the table
            id: Record ID to archive
            by: User/system performing the archive (default: 'system')
            timestamp: ISO timestamp (default: current UTC time)
            
        Returns:
            True if successful
        """
        if self.engine is None or self.metadata is None:
            raise Exception("Database not connected. Call log_in() first.")
        
        # Load table if not in metadata
        if table_name not in self.metadata.tables:
            table = Table(table_name, self.metadata, autoload_with=self.engine)
        else:
            table = self.metadata.tables[table_name]

        if timestamp is None:
            timestamp = datetime.utcnow().isoformat()

        operation_info = {
            "operation": "archive",
            "by": by,
            "timestamp": self._parse_iso(timestamp)
        }

        record_update = {"archived": True}

        # Append to operations history if column exists
        if 'operations_history' in table.c:
            record_update['operations_history'] = func.jsonb_set(
                table.c.operations_history,
                '{history}',
                (table.c.operations_history['history'] + [operation_info]).cast(JSONB)
            )

        stmt = update(table).where(table.c.id == id).values(**record_update)

        with self.engine.begin() as conn:
            conn.execute(stmt)
        
        return True

    def handle_input(
        self, 
        message: bytes, 
        json_fields: Dict[str, str], 
        table_name: str
    ) -> bool:
        """
        Handle incoming message and perform appropriate database operation.
        
        Args:
            message: Raw message bytes
            json_fields: Field definitions for table creation
            table_name: Name of the target table
            
        Returns:
            True if operation successful, False otherwise
        """
        try:
            # Map datatypes and ensure table exists
            fields = self.map_datatypes(json_fields)
            table_created = self.create_table_if_not_exists(table_name, fields)
            
            if not table_created:
                return False

            # Parse message
            message_dict = json.loads(message.decode('utf-8'))
            operation = message_dict.get('operation')
            record_id = message_dict.get('odoo_internal_id')

            if operation == "create" or operation == "dump":
                if not self.exists_by_id(table_name, record_id):
                    return self.create_record(table_name, message_dict)
                    
            elif operation == "update":
                if self.exists_by_id(table_name, record_id):
                    return self.update_record(table_name, record_id, message_dict)
                else:
                    # Create if doesn't exist
                    return self.create_record(table_name, message_dict)
                    
            elif operation == "delete":
                if self.exists_by_id(table_name, record_id):
                    return self.set_archived_flag_record(table_name, record_id)
                else:
                    # Create then archive
                    self.create_record(table_name, message_dict)
                    return self.set_archived_flag_record(table_name, record_id)
            
            return False
            
        except Exception as e:
            print(f"Error handling input: {str(e)}")
            print(traceback.format_exc())
            return False