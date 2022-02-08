import os

db_file_path = os.environ.get("SUPERSET__SQLALCHEMY_DATABASE_URI", "")[
    len("sqlite://") :
]

if os.path.exists(db_file_path):
    os.remove(db_file_path)
