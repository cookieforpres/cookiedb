# CookieDB
A NoSQL database made in python.

### How to use package (API wrapper):
```text
Coming soon...
```

### How to start server:
```text
uvicorn main:app --port=33940

You can make the port whatever but in main.py the port is 33940
so if you change it in the uvicorn command change it in the file
or it will not print the right port.

When running the server, please make sure the first route you go to
is "/initialize". if you dont you will receive and error.
```

### How it works:
```text
When creating a database it makes a folder and in that folder is a
file named "database.json". In database.json there is information
on collections and the database its self. Now when creating a
collection is like creating a database but except everything is stored
in "collection.json". The collection.json file has all information
on the documents and collection its self. Its probably not the most
convenient way to make a database but you see all the different
databases and collections.
```

### Documentation:
```text
Coming soon...
```

***Note: This is completely for fun and has no real security so only use this for playing
around and not any real project or production :)***