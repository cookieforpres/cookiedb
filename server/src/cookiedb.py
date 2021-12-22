from .collection import Collection
from .database import Database
import datetime
import pathlib
import json
import os

class CookieDB:
    def __init__(self):
        self.database_count = 0
        self.__refresh_database_info()
        self.database = Database()
        self.collection = Collection()

    def __refresh_database_info(self):
        dir = os.path.join(__file__.replace('./', '')).split('/')[:-2]
        dir = '/'.join(dir)
        file = dir + f'/databases/'

        json_file = open(f'{dir}/databases/databases.json', 'r+')
        json_data = json.load(json_file)
        size = 0

        for base, dirs, files in os.walk(file):
            self.database_count = len(dirs)

            for f in files:
                if f != 'databases.json' or f != 'database.json':
                    fp = os.path.join(base, f)
                    size += os.path.getsize(fp)

        json_data['size'] = size
        json_data['updated_at'] = str(datetime.datetime.now())

        with open(f'{file}/databases.json', 'w+') as f:
            json.dump(json_data, f, indent=4)
            f.close()

    def initialize(self, max_size: int):
        dir = os.path.join(__file__.replace('./', '')).split('/')[:-2]
        dir = '/'.join(dir)
        file = dir + f'/databases/'

        if not os.path.exists(file):
            os.mkdir(file)

            database_json_file_name = f'{dir}/databases/databases.json'
            database_json_file = pathlib.Path(database_json_file_name)
            database_json_file.touch(exist_ok=True)

            with open(database_json_file, 'w+') as f:
                data = {
                    'max_size': max_size,
                    'size': 0,
                    'databases': [],
                    'created_at': str(datetime.datetime.now()),
                    'updated_at': str(datetime.datetime.now())
                }

                json.dump(data, f, indent=4)
                f.close()

            return {'message': 'initialized cookiedb server app'}

        else:
            return {'message': 'already initialized'}