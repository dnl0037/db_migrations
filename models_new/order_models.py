import enum
from sqlalchemy import Column, Integer, ForeignKey, DateTime, Numeric, Enum as SQLAlchemyEnum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import NewBase

from .user_models import User, Address
from .product_models import Product


class OrderStatusEnum(enum.Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    SHIPPED = "SHIPPED"
    DELIVERED = "DELIVERED"
    CANCELLED = "CANCELLED"
    REFUNDED = "REFUNDED"


class Order(NewBase):
    """Modelo para pedidos."""
    __tablename__ = "orders"

    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    order_date = Column(DateTime(timezone=True), default=func.now(), server_default=func.now(), nullable=False)
    status = Column(SQLAlchemyEnum(OrderStatusEnum), default=OrderStatusEnum.PENDING, server_default='PENDING',
                    nullable=False, index=True)

    # El total_amount podría ser calculado dinámicamente o almacenado.
    # Por ahora lo omitimos aquí y lo calcularemos en el script de migración o se podría añadir como campo calculado.
    # total_amount = Column(Numeric(10, 2), nullable=False)

    shipping_address_id = Column(Integer, ForeignKey("addresses.id"), nullable=False)
    billing_address_id = Column(Integer, ForeignKey("addresses.id"), nullable=True)  # Puede ser la misma que shipping

    # Relaciones
    user = relationship("User", back_populates="orders")
    items = relationship("OrderItem", back_populates="order", cascade="all, delete-orphan")

    shipping_address = relationship("Address", foreign_keys=[shipping_address_id], back_populates="shipping_for_orders")
    billing_address = relationship("Address", foreign_keys=[billing_address_id], back_populates="billing_for_orders")

    def __repr__(self):
        return f"<Order(id={self.id}, user_id={self.user_id}, status='{self.status.value}')>"


class OrderItem(NewBase):
    """Modelo para los ítems dentro de un pedido."""
    __tablename__ = "order_items"

    order_id = Column(Integer, ForeignKey("orders.id", ondelete="CASCADE"), nullable=False)
    product_id = Column(Integer, ForeignKey("products.id"),
                        nullable=False)  # onDelete='RESTRICT' or 'SET NULL' podría ser una opción
    quantity = Column(Integer, nullable=False)
    # Precio unitario al momento de la compra (puede diferir del precio actual del producto)
    unit_price_at_purchase = Column(Numeric(10, 2), nullable=False)

    # Relaciones
    order = relationship("Order", back_populates="items")
    product = relationship("Product", back_populates="order_items")

    def __repr__(self):
        return f"<OrderItem(id={self.id}, order_id={self.order_id}, product_id={self.product_id}, quantity={self.quantity})>"
