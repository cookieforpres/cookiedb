import datetime
import pathlib
import shutil
import json
import uuid
import os

class Database:
    def __init__(self):
        self.database_count = 0
        self.size = 0

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

        self.size = size

        json_data['size'] = size
        json_data['updated_at'] = str(datetime.datetime.now())

        with open(f'{file}/databases.json', 'w+') as f:
            json.dump(json_data, f, indent=4)
            f.close()

    def create(self, name: str, capped: bool = False, capped_amount: int = 512):
        database = {
            'id': str(uuid.uuid4()).replace('-', ''),
            'name': name, 'capped': capped,
            'capped_amount': capped_amount, 'collections': [],
            'created_at': str(datetime.datetime.now()),
            'updated_at': str(datetime.datetime.now())
        }

        dir = os.path.join(__file__.replace('./', '')).split('/')[:-2]
        dir = '/'.join(dir)
        file = f'{dir}/databases/{name}'

        if not os.path.exists(file):
            os.mkdir(file)

        else:
            return {'message': 'database already exist'}

        if not os.path.exists(f'{dir}/databases/{name}/database.json'):
            file = pathlib.Path(f'{dir}/databases/{name}/database.json')
            file.touch(exist_ok=True)

        try:
            with open(f'{dir}/databases/{name}/database.json', 'a+') as f:
                json.dump(database, f, indent=4)
                f.close()

            file = open(f'{dir}/databases/databases.json', 'r+')
            json_data = json.load(file)
            json_data['databases'].append(database)
            file.close()

            with open(f'{dir}/databases/databases.json', 'w+') as f:
                json.dump(json_data, f, indent=4)
                f.close()

        except TypeError:
            return {'message': 'error parsing data [1]'}

        except json.decoder.JSONDecodeError:
            return {'message': 'error parsing data [2]'}

        self.__refresh_database_info()
        return {'message': 'database created', 'database': database}

    def delete(self, name: str):
        dir = os.path.join(__file__.replace('./', '')).split('/')[:-2]
        dir = '/'.join(dir)
        file = f'{dir}/databases/{name}'

        if os.path.exists(file):
            shutil.rmtree(file)

            file = open(f'{dir}/databases/databases.json', 'r+')
            json_data = json.load(file)
            file.close()

            count = 0
            for obj in json_data['databases']:
                if obj['name'] == json_data['databases'][count]['name']:
                    if len(json_data['databases']) == 2:
                        json_data['databases'] = [json_data['databases'].pop(count)]

                    elif len(json_data['databases']) == 1:
                        json_data['databases'].pop(count)
                        json_data = {
                            'max_size': json_data['max_size'],
                            'size': json_data['size'],
                            'created_at': json_data['created_at'],
                            'updated_at': json_data['updated_at'],
                            'databases': []
                        }

                    else:
                        json_data['databases'] = json_data['databases'].pop(count)

                count += 1

            with open(f'{dir}/databases/databases.json', 'w+') as f:
                json.dump(json_data, f, indent=4)
                f.close()

            self.__refresh_database_info()
            return {'message': 'database deleted'}

        else:
            return {'message': 'cant find database'}

    def find(self):
        dir = os.path.join(__file__.replace('./', '')).split('/')[:-2]
        dir = '/'.join(dir)
        file = f'{dir}/databases/databases.json'

        json_data = open(file, 'r+')
        json_data = json.load(json_data)

        databases_names = []
        for db in json_data['databases']:
            databases_names.append(db['name'])

        self.__refresh_database_info()
        return {'message': 'databases found', 'databases': databases_names}

    def find_one(self, name: str):
        dir = os.path.join(__file__.replace('./', '')).split('/')[:-2]
        dir = '/'.join(dir)
        file = f'{dir}/databases/{name}'

        if os.path.exists(file):
            with open(f'{file}/database.json', 'r+') as f:
                data = json.load(f)
                f.close()

                self.__refresh_database_info()
                return {'message': 'database found', 'database': data}

        else:
            return {'message': 'cant find database'}