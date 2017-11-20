# noteql
Doing sql in notebooks.

The main purpose is a quick and easy way to get analysing data in postgreql.

# Installation With Docker

You need docker and docker-compose to make this work. 
Clone this repo and run ```./start``` to get it up and running, this will take a long while the first time. 
To stop it ```./stop``` and if you pull changed code from this repo then you may neet to run ```./rebuild```.

Jupyter Notebook -  http://localhost:12121
Postsql Admin url -  http://localhost:12122  Username: noteql Password: noteql

On windows you have to make sure the directory you clone this repo in the users home directory. Also for you may to replace localhost with the ip given by the output of running docker-machine.exe ip

# Installation Without Docker

## Installation of postgresql
You need to install a late 9.6+ version of postgresql. To do this goto: 

https://www.enterprisedb.com/downloads/postgres-postgresql-downloads

and follow instructions.  This will install pgadmin4 desktop utility.

Using pgadmin you should make a new database and a new user/password that owns that database. For example you can call the database/user/password all noteql.


## Installation of dependancies

Make sure python 3.5+ is installed with virualenv.
Clone this repo anywhere you like and make a virtual environment and install the requirements.
```
git clone https://github.com/OpenDataServices/noteql.git
cd noteql
virtualenv -p python3 .ve  # if you have just python 3 installed you can miss out the -p python3
source .ve/bin/activate # for windows run this .ve\Scripts\activate.bat 
pip install -r requirements.txt
```

## Running

Jupyter notebooks should be installed so just run:
```
jupyter notebook
```

The only difference is that when creating a session you have to add the dburi (which is a sqlalchemy uri) when you setup a session. i.e.

```
#If you create a supereuser noteql with password and db noteql use this line instead
session = noteql.Session('newexample', dburi='postgresql://noteql:noteql@localhost/noteql')

#Use this line if you have a custom postges setup replacing user, password and mydatabase
session = noteql.Session('newexample', dburi='postgresql://user:password@localhost/mydatabase')
```

# Usage

TODO
Look in files/example/test.ipynb for an example.


