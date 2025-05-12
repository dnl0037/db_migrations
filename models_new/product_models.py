from sqlalchemy import Column, String, Text, Numeric, Integer, ForeignKey, text
from sqlalchemy.orm import relationship
from .base import NewBase


class ProductCategory(NewBase):
    """Modelo para categorías de productos."""
    __tablename__ = "product_categories"

    name = Column(String(100), unique=True, index=True, nullable=False)
    description = Column(Text, nullable=True)

    # Relación
    products = relationship("Product", back_populates="category")

    def __repr__(self):
        return f"<ProductCategory(id={self.id}, name='{self.name}')>"


class Product(NewBase):
    """Modelo para productos."""
    __tablename__ = "products"

    name = Column(String(200), index=True, nullable=False)
    description = Column(Text, nullable=True)
    price = Column(Numeric(10, 2), nullable=False)  # Numeric para precisión, ej: 12345678.90
    sku = Column(String(50), unique=True, index=True, nullable=False)  # Stock Keeping Unit
    stock_quantity = Column(Integer, default=0, server_default=text("0"), nullable=False)

    category_id = Column(Integer, ForeignKey("product_categories.id"), nullable=False)

    # Relaciones
    category = relationship("ProductCategory", back_populates="products")
    order_items = relationship("OrderItem", back_populates="product")

    def __repr__(self):
        return f"<Product(id={self.id}, name='{self.name}', price={self.price})>"
