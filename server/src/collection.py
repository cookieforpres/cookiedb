import datetime
import pathlib
import json
import uuid
import os

class Collection:
    def __init__(self):
        self.database_count = 0
        self.initialized = False
        self.__refresh_database_info()

    def __refresh_database_info(self):
        dir = os.path.join((__file__).replace('./', '')).split('/')[:-2]
        dir = '/'.join(dir)
        file = dir + f'/databases/'

        json_file = open(f'{dir}/databases/databases.json', 'r+')
        json_data = json.load(json_file)
        size = 0

        for base, dirs, files in os.walk(file):
            self.database_count = len(dirs)

            for f in files:
                if f != 'databases.json':
                    fp = os.path.join(base, f)
                    size += os.path.getsize(fp)

        json_data['size'] = size
        json_data['updated_at'] = str(datetime.datetime.now())

        for db in json_data['databases']:
            for col in db['collections']:
                col['updated_at'] = str(datetime.datetime.now())

        with open(f'{file}/databases.json', 'w+') as f:
            json.dump(json_data, f, indent=4)
            f.close()

    def create(self, name: str, database: str):
        collection = {
            'id': str(uuid.uuid4()).replace('-', ''),
            'parent': database,
            'name': name, 'schema': {}, 'documents': [],
            'created_at': str(datetime.datetime.now()),
            'updated_at': str(datetime.datetime.now())
        }

        dir = os.path.join((__file__).replace('./', '')).split('/')[:-2]
        dir = '/'.join(dir)
        file = dir + f'/databases/{database}/{name}'

        if not os.path.exists(file):
            os.mkdir(file)

        else:
            return {'message': 'collections already exist'}

        if not os.path.exists(f'{file}/collection.json'):
            file = pathlib.Path(f'{file}/collection.json')
            file.touch(exist_ok=True)

        try:
            with open(file, 'a+') as f:
                json.dump(collection, f, indent=4)
                f.close()

            file = open(f'{dir}/databases/{database}/database.json', 'r+')
            json_data = json.load(file)
            json_data['collections'].append(collection)
            file.close()

            with open(f'{dir}/databases/{database}/database.json', 'w+') as f:
                json.dump(json_data, f, indent=4)
                f.close()

            file = open(f'{dir}/databases/databases.json', 'r+')
            json_data = json.load(file)

            for db in json_data['databases']:
                if db['name'] == database:
                    db['collections'].append(collection)

            file.close()

            with open(f'{dir}/databases/databases.json', 'w+') as f:
                json.dump(json_data, f, indent=4)
                f.close()

        except TypeError:
            return {'message': 'error parsing data [1]'}

        except json.decoder.JSONDecodeError:
            return {'message': 'error parsing data [2]'}

        self.__refresh_database_info()
        return {'message': 'collection created', 'collection': collection}

    def find_collections(self, database: str):
        dir = os.path.join((__file__).replace('./', '')).split('/')[:-2]
        dir = '/'.join(dir)
        file = dir + f'/databases/{database}/database.json'

        json_data = open(file, 'r+')
        json_data = json.load(json_data)

        collection_names = []
        for col in json_data['collections']:
            collection_names.append(col['name'])

        self.__refresh_database_info()
        return {'message': 'collections found', 'collections': collection_names}

    def find_collection(self, name: str, database: str):
        pass

    def insert_one(self, collection: str, database: str, filter: object):
        pass

    def insert_many(self, collection: str, database: str, filter: list):
        pass

    def delete_one(self):
        pass

    def delete_many(self):
        pass

    def find_one(self):
        pass

    def find_many(self):
        pass

    def update_one(self):
        pass

    def update_many(self):
        pass