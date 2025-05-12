from sqlalchemy import Column, Integer, String, Text, DateTime  # DateTime no lo usaremos mal aquí
from sqlalchemy.sql import func  # Para default timestamps
from config import OldBase


class OldUser(OldBase):
    """
    Tabla de usuarios en el sistema antiguo.
    Malas prácticas:
    - registration_date_str: Fecha como string.
    - address_combined: Dirección completa en un solo campo, difícil de consultar por partes.
    - No hay índice en email, lo que haría lentas las búsquedas por email.
    """
    __tablename__ = "old_users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, index=True, nullable=False)
    email = Column(String(100), unique=True)  # Sin index=True a propósito para simular lentitud
    full_name = Column(String(100))
    # MALA PRÁCTICA: Fecha como string
    registration_date_str = Column(String(20))
    # MALA PRÁCTICA: Dirección como un solo campo de texto largo
    address_combined = Column(Text)
    # MALA PRÁCTICA: Número de teléfono podría tener su propio formato o validación, aquí solo un string
    phone_number_str = Column(String(20))


class OldProduct(OldBase):
    """
    Tabla de productos en el sistema antiguo.
    Malas prácticas:
    - price_str: Precio como string.
    - category_name: Nombre de categoría directamente, causando redundancia si muchos productos
                     comparten la misma categoría. Debería ser una tabla separada con FK.
    - No hay tracking de stock real, solo una descripción.
    """
    __tablename__ = "old_products"

    id = Column(Integer, primary_key=True, index=True)
    product_name = Column(String(100), nullable=False, index=True)
    description = Column(Text)
    # MALA PRÁCTICA: Precio como string
    price_str = Column(String(20))  # Ejemplo: "25.99 USD", "100 EUR"
    # MALA PRÁCTICA: Categoría como string, no normalizada
    category_name_redundant = Column(String(50))
    # MALA PRÁCTICA: Fecha de creación como string
    created_at_str = Column(String(20))


class OldOrder(OldBase):
    """
    Tabla de pedidos en el sistema antiguo.
    Malas prácticas:
    - user_identifier: Podría ser el username o el email, inconsistente. No es una FK al ID de usuario.
    - product_details_json: Lista de productos como JSON o texto, difícil de consultar y agregar.
                       Debería ser una tabla de items de pedido (OrderLine).
    - total_amount_str: Monto total como string.
    - order_date_str: Fecha como string.
    - status: String libre, podría tener inconsistencias ("Shipped", "shipped", "Enviado").
    """
    __tablename__ = "old_orders"

    id = Column(Integer, primary_key=True, index=True)
    # MALA PRÁCTICA: Identificador de usuario como texto, no FK. Podría ser username, email, etc.
    user_identifier_text = Column(String(100), index=True)
    # MALA PRÁCTICA: Fecha de pedido como string
    order_date_str = Column(String(20))
    # MALA PRÁCTICA: Estado del pedido como texto libre
    status_text = Column(String(20))  # e.g., "pending", "shipped", "delivered", "Pending"
    # MALA PRÁCTICA: Detalles del producto como texto o JSON. Aquí usaremos texto simple para simularlo.
    # Imaginemos que esto guarda algo como: "ProductA (2) @ 10.00; ProductB (1) @ 25.50"
    # O peor, solo una lista de nombres de productos: "Laptop, Mouse, Teclado"
    # Para este ejemplo, vamos a simplificar y solo asociar un producto y cantidad por "pedido"
    # simulando que una tabla "OrderItems" no existe y la info está mal distribuida.
    # Esta es una simplificación, en la realidad podría ser un campo JSON o XML.
    # VAMOS A HACERLO PEOR: REDUNDANCIA
    product_name_redundant = Column(String(100))  # Nombre del producto duplicado
    quantity = Column(Integer)
    # MALA PRÁCTICA: Precio unitario redundante y como string
    unit_price_str_redundant = Column(String(20))
    # MALA PRÁCTICA: Total del pedido como string
    total_order_amount_str = Column(String(20))  # Debería ser calculado

# Nota: No estamos definiendo relaciones explícitas (ForeignKey) a propósito.
# Las relaciones son "implícitas" y se basan en datos que podrían ser inconsistentes.
