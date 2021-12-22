from colorama import Fore
from cookiedb import *
import fastapi
import time
import os

HOST = '127.0.0.1'
PORT = 33940

app = fastapi.FastAPI()
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
def initialize(max_cap):
    return cookiedb.initialize(int(max_cap))

@app.get('/databases')
def databases():
    return cookiedb.Database().find()

@app.get('/database/{name}')
def database(name):
    return cookiedb.Database().find_one(name)

@app.get('/databases/create/{name}')
def create_database(name):
    return cookiedb.Database().create(name)

@app.get('/databases/delete/{name}')
def delete_database(name):
    return cookiedb.Database().delete(name)

