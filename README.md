Usage: 
  Check ```Creds``` class in ```db_operations.py``` and use credentials for PostgreSQL server.
  
  Then create tables and fill them with data:
    ```python set_up_data.py```

  Then just start server:
    ```python server.py```

  Everything should be working, otherwise check exceptions.

Testing:
  Pytests are located in ```test_db_operations.py```, simply run the script with ```python test_db_operations.py``` and check if anything fails.
