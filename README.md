# Simulated Project: Professional Database Migration

This project simulates a professional scenario of migrating an "outdated" and poorly structured PostgreSQL database to a
new, well-designed, and normalized PostgreSQL database. It uses Python, SQLAlchemy, Alembic, and Pandas.

## General Objective

Understand the complete cycle of data migration in a professional context—from designing and populating a problematic
database, through designing a new schema, to implementing a robust data migration script while applying software
engineering best practices.

## Technologies Used

* Python 3.11+
* SQLAlchemy (ORM and Core)
* Alembic (for schema migrations of the new DB)
* PostgreSQL (as the database management system)
* python-dotenv (for managing environment variables)
* Faker (for massive test data generation)
* Pandas (for data transformation in one version of the migration script)

## Project Structure

```text
db_migration_project/
├── migrations/                # Alembic directory for new_good_db
│   ├── versions/             # Alembic revision files
│   ├── env.py                # Alembic environment script
│   └── script.py.mako        # Template for new revisions
├── models_new/               # SQLAlchemy models for the new DB (new_good_db)
│   ├── __init__.py
│   ├── base.py               # Declarative base for new models
│   ├── order_models.py
│   ├── product_models.py
│   └── user_models.py
├── models_old/               # SQLAlchemy models for the old DB (old_bad_db)
│   └── old_models.py
├── scripts/                  # Project utility scripts
│   ├── populate_old_db.py    # To create and populate old_bad_db
│   ├── migrate_data.py       # Direct migration script
│   └── migrate_data_pandas.py # Migration script using Pandas
├── .env                      # Environment variables (DO NOT commit to Git)
├── .gitignore                # Specifies files to ignore with Git
├── alembic.ini               # Alembic configuration file
├── config.py                 # Loads config and DB URLs
├── requirements.txt          # Project Python dependencies
└── README.md                 # This file
```

## Environment Setup

1. **Clone the repository (if applicable).**
2. **Create and activate a virtual environment:**
    ```bash
    python -m venv venv
    # Windows
    # venv\Scripts\activate
    # macOS/Linux
    # source venv/bin/activate
    ```
3. **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
4. **Configure PostgreSQL:**
    * Make sure you have a running PostgreSQL server.
    * Create two databases (e.g., `old_bad_db` and `new_good_db`).
    * Create a PostgreSQL user with privileges to create tables and manipulate data in those databases.
5. **Configure environment variables:**
    * Copy or rename `.env.example` to `.env` (if an example exists; otherwise, create one).
    * Edit the `.env` file with your PostgreSQL credentials:
        ```env
        OLD_DB_USER="your_pg_user"
        OLD_DB_PASSWORD="your_pg_password"
        OLD_DB_HOST="localhost"
        OLD_DB_PORT="5432"
        OLD_DB_NAME="old_bad_db"

        NEW_DB_USER="your_pg_user"
        NEW_DB_PASSWORD="your_pg_password"
        NEW_DB_HOST="localhost"
        NEW_DB_PORT="5432"
        NEW_DB_NAME="new_good_db"
        ```

## Running the Project

Follow these steps in order:

1. **Populate the Old Database (`old_bad_db`):**  
   This script will create the tables in `old_bad_db` (as defined in `models_old/old_models.py`) and populate them with
   Faker-generated data.
    ```bash
    python scripts/populate_old_db.py
    ```
   Verify in your PostgreSQL client that the `old_users`, `old_products`, and `old_orders` tables have been created and
   filled.

2. **Set Up the New Database (`new_good_db`) with Alembic:**  
   These commands will create the tables in `new_good_db` (as defined in `models_new/`) using Alembic to manage the
   schema.
    * Review configuration in `alembic.ini` and `migrations/env.py` if needed.
    * Generate the initial migration (if not already in `migrations/versions/`):
        ```bash
        alembic revision -m "create_initial_tables" --autogenerate
        ```
    * Apply the migrations:
        ```bash
        alembic upgrade head
        ```
   Verify in your PostgreSQL client that the `new_good_db` tables (e.g., `users`, `products`, `orders`,
   `alembic_version`, etc.) have been created and are empty.

3. **Run the Data Migration Script:**  
   This script will read data from `old_bad_db`, transform it, and insert it into `new_good_db`.  
   Choose one of the two versions:

    * **Direct version using SQLAlchemy:**
        ```bash
        python scripts/migrate_data.py
        ```
    * **Version using Pandas for transformation:**
        ```bash
        python scripts/migrate_data_pandas.py
        ```
   Check the logs for any errors or warnings. Verify the data in `new_good_db`.

## Additional Considerations

* **Idempotency:** The migration scripts aim to be idempotent, meaning running them multiple times should not duplicate
  data (though the `clear_target_db=True` option in the scripts clears the new DB before each run).
* **Data Volume:** The scripts are designed to handle a substantial amount of data through batch processing and
  efficient reading.
* **Dirty Data:** The project simulates handling "dirty" data from the old database, with logic for parsing and
  transformation. Errors during this process are logged.
