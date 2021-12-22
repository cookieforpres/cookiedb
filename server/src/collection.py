import datetime
import pathlib
import shutil
import json
import uuid
import os

class Collection:
    def __init__(self):
        self.database_count = 0
        self.initialized = False
        self.__refresh_database_info()

    def __refresh_database_info(self):
        dir = os.path.join(__file__.replace('./', '')).split('/')[:-2]
        dir = '/'.join(dir)
        file = f'{dir}/databases/'

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

        for db in json_data['databases']:
            for col in db['collections']:
                col['updated_at'] = str(datetime.datetime.now())

        with open(f'{file}/databases.json', 'w+') as f:
            json.dump(json_data, f, indent=4)
            f.close()

    def create_collection(self, name: str, database: str):
        collection = {
            'id': str(uuid.uuid4()).replace('-', ''),
            'parent': database,
            'name': name, 'schema': {}, 'documents': [],
            'created_at': str(datetime.datetime.now()),
            'updated_at': str(datetime.datetime.now())
        }

        dir = os.path.join(__file__.replace('./', '')).split('/')[:-2]
        dir = '/'.join(dir)
        file = f'{dir}/databases/{database}/{name}'

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
        dir = os.path.join(__file__.replace('./', '')).split('/')[:-2]
        dir = '/'.join(dir)
        file = f'{dir}/databases/{database}/database.json'

        if os.path.exists(file):
            json_data = open(file, 'r+')
            json_data = json.load(json_data)

            collection_names = []
            for col in json_data['collections']:
                collection_names.append(col['name'])

            self.__refresh_database_info()
            return {'message': 'collections found', 'collections': collection_names}

        else:
            return {'message': 'cant find database'}

    def find_collection(self, name: str, database: str):
        dir = os.path.join(__file__.replace('./', '')).split('/')[:-2]
        dir = '/'.join(dir)
        database_file = f'{dir}/databases/{database}'
        collection_file = f'{dir}/databases/{database}/{name}'

        if os.path.exists(database_file):
            if os.path.exists(collection_file):
                with open(f'{collection_file}/collection.json', 'r+') as f:
                    collection = json.load(f)
                    f.close()

                    self.__refresh_database_info()
                    return {'message': 'collection found', 'collection': collection}

            else:
                return {'message': 'cant find collection'}

        else:
            return {'message': 'cant find database'}

    def delete_collection(self, name: str, database: str):
        dir = os.path.join(__file__.replace('./', '')).split('/')[:-2]
        dir = '/'.join(dir)
        database_file = f'{dir}/databases/{database}'
        collection_file = f'{dir}/databases/{database}/{name}'

        if os.path.exists(database_file):
            if os.path.exists(collection_file):
                shutil.rmtree(collection_file)

                self.__refresh_database_info()
                return {'message': 'collection deleted'}

            else:
                return {'message': 'cant find collection'}

        else:
            return {'message': 'cant find database'}

    def insert_one(self, collection: str, database: str, document: dict):
        dir = os.path.join(__file__.replace('./', '')).split('/')[:-2]
        dir = '/'.join(dir)
        database_file = f'{dir}/databases/{database}'
        collection_file = f'{dir}/databases/{database}/{collection}'

        if 'id' not in document.keys():
            new_document = {'id': str(uuid.uuid4())}

            for key, value in document.items():
                new_document[key] = value

            document = new_document

        if os.path.exists(database_file):
            if os.path.exists(collection_file):
                if os.path.exists(f'{collection_file}/collection.json'):
                    file = open(f'{collection_file}/collection.json', 'r+')
                    json_data = json.load(file)
                    file.close()

                    json_data['documents'].append(document)

                    with open(f'{collection_file}/collection.json', 'w+') as f:
                        json.dump(json_data, f, indent=4)


                    db_file = open(f'{database_file}/database.json', 'r+')
                    db_json_data = json.load(db_file)
                    db_file.close()

                    for col in db_json_data['collections']:
                        if col['name'] == collection:
                            col['updated_at'] = str(datetime.datetime.now())
                            col['documents'] = json_data['documents']

                    with open(f'{database_file}/database.json', 'w+') as f:
                        json.dump(db_json_data, f, indent=4)
                        f.close()

                    self.__refresh_database_info()
                    return {'message': 'document inserted', 'document': document}

                else:
                    return {'message': 'cant find collection data file'}

            else:
                return {'message': 'cant find collection'}

        else:
            return {'message': 'cant find database'}

    def insert_many(self, collection: str, database: str, documents: list):
        dir = os.path.join(__file__.replace('./', '')).split('/')[:-2]
        dir = '/'.join(dir)
        database_file = f'{dir}/databases/{database}'
        collection_file = f'{dir}/databases/{database}/{collection}'

        new_documents = []
        for doc in documents:
            if 'id' not in doc.keys():
                new_document = {'id': str(uuid.uuid4())}

                for key, value in doc.items():
                    new_document[key] = value

                doc = new_document
                new_documents.append(doc)

        if os.path.exists(database_file):
            if os.path.exists(collection_file):
                if os.path.exists(f'{collection_file}/collection.json'):
                    file = open(f'{collection_file}/collection.json', 'r+')
                    json_data = json.load(file)
                    file.close()

                    for doc in new_documents:
                        json_data['documents'].append(doc)

                    with open(f'{collection_file}/collection.json', 'w+') as f:
                        json.dump(json_data, f, indent=4)

                    db_file = open(f'{database_file}/database.json', 'r+')
                    db_json_data = json.load(db_file)
                    db_file.close()

                    for col in db_json_data['collections']:
                        if col['name'] == collection:
                            col['updated_at'] = str(datetime.datetime.now())
                            col['documents'] = json_data['documents']

                    with open(f'{database_file}/database.json', 'w+') as f:
                        json.dump(db_json_data, f, indent=4)
                        f.close()

                    self.__refresh_database_info()
                    return {'message': 'documents inserted', 'documents': new_documents}

                else:
                    return {'message': 'cant find collection data file'}

            else:
                return {'message': 'cant find collection'}

        else:
            return {'message': 'cant find database'}

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