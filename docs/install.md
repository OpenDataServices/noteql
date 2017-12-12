# Installation Without Docker

## Installation of postgresql
You need to install a late 9.6+ version of postgresql. The simplist way is from [enterprisedb](https://www.enterprisedb.com/downloads/postgres-postgresql-downloads)
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

```python
#If you create a supereuser noteql with password and db noteql use this line instead
session = noteql.Session('newexample', dburi='postgresql://noteql:noteql@localhost/noteql')

#Use this line if you have a custom postges setup replacing user, password and mydatabase
session = noteql.Session('newexample', dburi='postgresql://user:password@localhost/mydatabase')
```
