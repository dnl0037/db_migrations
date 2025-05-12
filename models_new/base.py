from sqlalchemy import Column, DateTime, Integer
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy.sql import func


@as_declarative()
class NewBase:
    """
    Clase base para los modelos de la nueva base de datos.
    Automáticamente añade una clave primaria 'id' si no se define.
    También añade campos 'created_at' y 'updated_at'.
    """
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)

    # @declared_attr
    # def __tablename__(cls):
    #     # Convierte el nombre de la clase de CamelCase a snake_case para el nombre de la tabla
    #     # Ejemplo: UserProfile -> user_profiles
    #     import re
    #     return re.sub(r'(?<!^)(?=[A-Z])', '_', cls.__name__).lower() + "s"

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
