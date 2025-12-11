from abc import ABC, abstractmethod
from typing import Any
import json

class DatabaseSync(ABC):
    @abstractmethod
    def log_in(self, host, port, user, password, database_name, any_other_credential=None) -> Any:
        pass

    @abstractmethod
    def map_datatypes(self, fields)-> dict[str, str]:
        pass

    @abstractmethod
    def create_table_if_not_exists(self, table_name, fields)-> bool:
        pass

    @abstractmethod
    def exists_by_id(self, table_name, id) -> bool:
        pass

    @abstractmethod
    def create_record(self, table_name, data) -> bool:
        pass

    @abstractmethod
    def update_record(self, table_name, id, data) -> bool:
        pass

    @abstractmethod
    def set_archived_flag_record(self, table_name, id) -> bool:
        pass

    @abstractmethod
    def handle_input(message, json_fields, table_name) -> bool:
        fields = self.map_datatypes(json_fields)
        table = self.create_table_if_not_exists(table_name, fields)
        if table:
            message_dict = json.loads(message.decode('utf-8'))
            if message_dict['operation'] == "create":
                if not self.exists_by_id(table_name, message_dict['id']):
                    self.create_record(table_name, message_dict)
                    return True
            elif message_dict['operation'] == "update":
                if self.exists_by_id(table_name, message_dict['id']):
                    self.update_record(table_name, message_dict['id'], message_dict)
                    return True
                else:
                    self.create_record(table_name, message_dict)
                    return True
            elif message_dict['operation'] == "delete":
                if self.exists_by_id(table_name, message_dict['id']):
                    self.set_archived_flag_record(table_name, message_dict['id'])
                    return True
                else:
                    self.create_record(table_name, message_dict)
                    self.set_archived_flag_record(table_name, message_dict['id'])
                    return True
            return False
        else:
            return False
                