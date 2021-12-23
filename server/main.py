from fastapi import FastAPI, Request
from colorama import Fore
from src import CookieDB
import time
import os

HOST = '127.0.0.1'
PORT = 33940

app = FastAPI()
cookiedb = CookieDB()
clear_console = lambda: os.system('cls' if os.name in ('nt', 'dos') else 'clear')

def string_space_corrector(string: str, max: int, start: bool=False):
    space = ''
    space_length = max - len(string)

    for _ in range(0, space_length):
        space += ' '

    if start:
        return f'{space}{string}'

    return f'{string}{space}'

host = string_space_corrector(HOST, 15)
port = string_space_corrector(str(PORT), 5)
start = string_space_corrector(str(time.strftime('%I:%M %p')), 8)

print(f'╔════════════════════════════════════════╗')
print(f'║ Welcome to {Fore.LIGHTMAGENTA_EX}CookieDB{Fore.RESET} Server App!        ║')
print(f'║ Made by cookie-for-pres on github      ║')
print(f'║                                        ║')
print(f'║ Host: {Fore.LIGHTMAGENTA_EX}{host}{Fore.RESET}                  ║')
print(f'║ Port: {Fore.LIGHTMAGENTA_EX}{port}{Fore.RESET}                            ║')
print(f'║ Start: {Fore.LIGHTMAGENTA_EX}{start}{Fore.RESET}                        ║')
print(f'╚════════════════════════════════════════╝')

@app.get('/initialize/{max_cap}')
def initialize(max_cap: int):
    return cookiedb.initialize(max_cap)

@app.get('/databases')
def databases():
    return cookiedb.database.find()

@app.get('/database/{database}')
def database(database: str):
    return cookiedb.database.find_one(database)

@app.get('/databases/create/{database}')
def create_database(database: str):
    return cookiedb.database.create(database)

@app.get('/databases/delete/{database}')
def delete_database(database: str):
    return cookiedb.database.delete(database)

@app.get('/collections/{database}')
def collections(database: str):
    return cookiedb.collection.find_collections(database)

@app.get('/collection/{database}/{collection}')
def collection(database: str, collection: str):
    return cookiedb.collection.find_collection(collection, database)

@app.get('/collections/create/{database}/{collection}')
def create_collection(database: str, collection: str):
    return cookiedb.collection.create_collection(collection, database)

@app.get('/collections/delete/{database}/{collection}')
def delete_collection(database: str, collection: str):
    return cookiedb.collection.delete_collection(collection, database)

@app.get('/collection/insert-one/{database}/{collection}')
async def insert_one_collection(req: Request, database: str, collection: str):
    document = await req.json()
    return cookiedb.collection.insert_one(collection, database, document)

@app.get('/collection/insert-many/{database}/{collection}')
async def insert_many_collection(req: Request, database: str, collection: str):
    document = await req.json()
    return cookiedb.collection.insert_many(collection, database, document)

@app.get('/collection/delete-one/{database}/{collection}')
async def delete_one(req: Request, database: str, collection: str):
    filter = await req.json()
    return cookiedb.collection.delete_one(collection, database, filter)

@app.get('/collection/update-one/{database}/{collection}')
async def update_one(req: Request, database: str, collection: str):
    filter,