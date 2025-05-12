from sqlalchemy import Column, String, DateTime, Boolean, ForeignKey, Integer, sql
from sqlalchemy.orm import relationship
from .base import NewBase


class User(NewBase):
    """Modelo para los usuarios del sistema nuevo."""
    __tablename__ = 'users'

    username = Column(String(50), unique=True, index=True, nullable=False)
    email = Column(String(120), unique=True, index=True, nullable=False)
    full_name = Column(String(100), nullable=True)
    hashed_password = Column(String(255), nullable=False)  # Almacenaremos contraseñas hasheadas
    is_active = Column(Boolean, nullable=False, default=True, server_default=sql.expression.true())
    is_superuser = Column(Boolean, nullable=False, default=False, server_default=sql.expression.false())
    registration_date = Column(DateTime(timezone=True), nullable=False)  # Antes registration_date_str
    phone_number = Column(String(20), nullable=True)  # Antes phone_number_str

    # Relaciones
    addresses = relationship("Address", back_populates="user", cascade="all, delete-orphan")
    orders = relationship("Order", back_populates="user")

    def __repr__(self):
        return f"<User(id={self.id}, username='{self.username}', email='{self.email}')>"


class Address(NewBase):
    """Modelo para las direcciones de los usuarios."""
    __tablename__ = "addresses"  # (generado por NewBase)

    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False,
                     index=True)  # FK a la tabla 'users'
    street = Column(String(255), nullable=False)
    city = Column(String(100), nullable=False)
    state = Column(String(100), nullable=True)
    zip_code = Column(String(20), nullable=False)
    country = Column(String(100), nullable=False)
    is_default_shipping = Column(Boolean, default=False, server_default=sql.expression.false(), nullable=False)
    is_default_billing = Column(Boolean, default=False, server_default=sql.expression.false(), nullable=False)

    # Relación
    user = relationship("User", back_populates="addresses")

    # Relaciones para pedidos (dirección de envío/facturación)
    # Una dirección puede ser usada en múltiples pedidos como dirección de envío
    shipping_for_orders = relationship("Order", foreign_keys="Order.shipping_address_id",
                                       back_populates="shipping_address")
    # Una dirección puede ser usada en múltiples pedidos como dirección de facturación
    billing_for_orders = relationship("Order", foreign_keys="Order.billing_address_id",
                                      back_populates="billing_address")

    def __repr__(self):
        return f"<Address(id={self.id}, user_id={self.user_id}, city='{self.city}')>"
