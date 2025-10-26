# Import file at src/db.py
from src import db

# Import settings
from src.config import root_dir

# Import DbObject from src/db.py
from src.db import DbObject


def main():
    obj: DbObject = db.init(root_dir)
    print(obj)


if __name__ == "__main__":
    main()
