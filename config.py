import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

load_dotenv()

# OLD DB
OLD_DB_USER = os.getenv("OLD_DB_USER")
OLD_DB_PASSWORD = os.getenv("OLD_DB_PASSWORD")
OLD_DB_HOST = os.getenv("OLD_DB_HOST")
OLD_DB_PORT = os.getenv("OLD_DB_PORT")
OLD_DB_NAME = os.getenv("OLD_DB_NAME")

OLD_DATABASE_URL = f"postgresql://{OLD_DB_USER}:{OLD_DB_PASSWORD}@{OLD_DB_HOST}:{OLD_DB_PORT}/{OLD_DB_NAME}"
old_engine = create_engine(OLD_DATABASE_URL, echo=False)  # echo=True para ver las SQL queries generadas
OldSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=old_engine)
OldBase = declarative_base()


def get_old_db():
    db = OldSessionLocal()
    try:
        yield db
    finally:
        db.close()


# NEW DB
NEW_DB_USER = os.getenv("NEW_DB_USER")
NEW_DB_PASSWORD = os.getenv("NEW_DB_PASSWORD")
NEW_DB_HOST = os.getenv("NEW_DB_HOST")
NEW_DB_PORT = os.getenv("NEW_DB_PORT")
NEW_DB_NAME = os.getenv("NEW_DB_NAME")

NEW_DATABASE_URL = f"postgresql://{NEW_DB_USER}:{NEW_DB_PASSWORD}@{NEW_DB_HOST}:{NEW_DB_PORT}/{NEW_DB_NAME}"
new_engine = create_engine(NEW_DATABASE_URL, echo=False)
NewSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=new_engine)


def get_new_db():
    db = NewSessionLocal()
    try:
        yield db
    finally:
        db.close()


if __name__ == "__main__":
    print("Intentando conectar a la base de datos ANTIGUA...")
    try:
        conn_old = old_engine.connect()
        conn_old.close()
        print(f"Conexión exitosa a: {OLD_DATABASE_URL.replace(OLD_DB_PASSWORD, '****')}")
    except Exception as e:
        print(f"Error conectando a la base de datos ANTIGUA: {e}")

    print("\nIntentando conectar a la base de datos NUEVA...")
    try:
        conn_new = new_engine.connect()
        conn_new.close()
        print(f"Conexión exitosa a: {NEW_DATABASE_URL.replace(NEW_DB_PASSWORD, '****')}")
    except Exception as e:
        print(f"Error conectando a la base de datos NUEVA: {e}")
