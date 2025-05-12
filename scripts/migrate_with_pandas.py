import logging
import os
import sys
from datetime import datetime
from decimal import Decimal, InvalidOperation
import re
import pandas as pd
import numpy as np

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import IntegrityError

# Añadir la raíz del proyecto al PYTHONPATH
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config import OLD_DATABASE_URL, NEW_DATABASE_URL
from models_new.base import NewBase
from models_new import User, Address, ProductCategory, Product, Order, OrderItem, OrderStatusEnum

# No necesitamos los modelos antiguos si leemos directamente con pd.read_sql

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Motores y Sesiones
old_db_engine = create_engine(OLD_DATABASE_URL)
new_db_engine = create_engine("")
NewSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=new_db_engine)

# --- MAPAS GLOBALES PARA IDs (aún necesarios si no se guardan en DFs) ---
# Opcional: podríamos añadir columnas 'new_id' a los DataFrames después de la inserción.
# Por ahora, mantendremos mapas separados para claridad en el ejemplo,
# pero integrar esto en los DFs es una buena optimización.
map_old_user_id_to_new_user_id = {}
map_new_user_username_to_new_id = {}
map_new_user_id_to_default_address_id = {}
map_old_category_name_to_new_category_id = {}
map_old_product_id_to_new_product_id = {}
map_old_product_name_to_new_product_id = {}


# --- FUNCIONES DE TRANSFORMACIÓN (similares, pero ahora aplicadas a Series/DataFrames de Pandas) ---

def normalize_category_name_pd(series: pd.Series) -> pd.Series:
    return series.str.strip().str.title().fillna("Unknown Category")


def parse_date_string_pd(series: pd.Series, formats: list) -> pd.Series:
    parsed_series = pd.Series([None] * len(series), index=series.index, dtype='datetime64[ns]')
    for fmt in formats:
        try_parse = pd.to_datetime(series, format=fmt, errors='coerce')
        parsed_series = parsed_series.fillna(try_parse)
    # Loguear los que no se pudieron parsear
    # unparsed_mask = parsed_series.isna() & series.notna()
    # if unparsed_mask.any():
    #     logger.warning(f"No se pudieron parsear fechas: {series[unparsed_mask].unique().tolist()} con formatos {formats}")
    return parsed_series


def parse_price_string_pd(series: pd.Series) -> pd.Series:
    def convert_price(price_val):
        if pd.isna(price_val):
            return None
        cleaned_price = str(price_val).upper().replace("USD", "").replace("EUR", "").replace("$", "").strip()
        try:
            return Decimal(cleaned_price)
        except InvalidOperation:
            logger.warning(f"No se pudo parsear el precio: '{price_val}' a Decimal.")
            return None  # O Decimal('0.00')

    return series.apply(convert_price)


def parse_combined_address_pd(address_combined: str) -> pd.Series:
    # Misma lógica que antes, pero para retornar una Serie para `apply`
    # (Ver la implementación anterior de `parse_combined_address` para la lógica detallada)
    # Esta función se aplicará fila por fila al DataFrame.
    parts = {"street": None, "city": None, "state": None, "zip_code": None, "country": None}
    if pd.isna(address_combined):
        return pd.Series(parts)
    try:
        # ... (lógica de parseo copiada de la función original) ...
        street_part, rest_of_address = address_combined.split(',', 1)
        parts["street"] = street_part.strip()
        city_part, state_zip_country_part = rest_of_address.split(',', 1)
        parts["city"] = city_part.strip()
        if ',' in state_zip_country_part:
            state_zip_part, country_part = state_zip_country_part.rsplit(',', 1)
            parts["country"] = country_part.strip()
        else:
            state_zip_part = state_zip_country_part.strip()
            if len(state_zip_part.split()) > 3 and not any(char.isdigit() for char in state_zip_part.split()[-1]):
                parts["country"] = state_zip_part.split()[-1]
                state_zip_part = " ".join(state_zip_part.split()[:-1])
        zip_match = re.search(r'\b(\d{5}(-\d{4})?)\b', state_zip_part)
        if zip_match:
            parts["zip_code"] = zip_match.group(1)
            parts["state"] = state_zip_part[:zip_match.start()].strip()
        else:
            parts["state"] = state_zip_part.strip()
            if len(parts["state"]) > 50: parts["state"] = parts["state"][:50]
        for key, value in parts.items():
            if value and len(value) == 0: parts[key] = None
        if not parts["street"] or not parts["city"] or not parts["zip_code"] or not parts["country"]:
            logger.warning(f"Parseo incompleto de dirección (Pandas): '{address_combined}'. Resultado: {parts}")
    except Exception as e:
        logger.error(f"Error parseando dirección (Pandas) '{address_combined}': {e}. Usando N/A.")
        parts["street"] = address_combined[:255] if pd.notna(address_combined) else "N/A"
        parts["city"] = parts.get("city") or "N/A"
        parts["zip_code"] = parts.get("zip_code") or "00000"
        parts["country"] = parts.get("country") or "Unknown"
    return pd.Series(parts)


# --- FUNCIONES DE MIGRACIÓN CON PANDAS ---
BATCH_INSERT_SIZE = 500  # Para inserciones en la nueva BD


def migrate_categories_pd(new_db: Session, df_old_products: pd.DataFrame):
    logger.info("Iniciando migración de categorías con Pandas...")
    unique_categories = df_old_products['category_name_redundant'].dropna().unique()
    df_new_categories = pd.DataFrame({'name_raw': unique_categories})
    df_new_categories['name'] = normalize_category_name_pd(df_new_categories['name_raw'])
    df_new_categories.drop_duplicates(subset=['name'], inplace=True)

    migrated_count = 0
    for index, row in df_new_categories.iterrows():
        # Chequear si ya existe para idempotencia
        existing = new_db.query(ProductCategory.id).filter(ProductCategory.name == row['name']).scalar()
        if existing:
            map_old_category_name_to_new_category_id[row['name_raw']] = existing
            map_old_category_name_to_new_category_id[row['name']] = existing  # Mapear nombre normalizado también
            continue

        category = ProductCategory(name=row['name'], description=f"Categoría para {row['name']}")
        try:
            new_db.add(category)
            new_db.flush()  # Para obtener el ID
            map_old_category_name_to_new_category_id[row['name_raw']] = category.id
            map_old_category_name_to_new_category_id[row['name']] = category.id
            migrated_count += 1
        except IntegrityError:
            new_db.rollback()  # importante
            logger.error(f"Error de integridad insertando categoría {row['name']}. Recuperando si existe...")
            existing = new_db.query(ProductCategory.id).filter(ProductCategory.name == row['name']).scalar()
            if existing:
                map_old_category_name_to_new_category_id[row['name_raw']] = existing
                map_old_category_name_to_new_category_id[row['name']] = existing
        except Exception as e:
            new_db.rollback()
            logger.error(f"Error migrando categoría '{row['name']}': {e}")

    new_db.commit()
    logger.info(f"Migración de categorías (Pandas) finalizada. {migrated_count} nuevas categorías creadas.")
    # logger.debug(f"Mapa de categorías: {map_old_category_name_to_new_category_id}")
    return df_new_categories  # Podríamos añadir 'new_category_id' al df y retornarlo


def migrate_users_and_addresses_pd(new_db: Session, df_old_users: pd.DataFrame):
    logger.info("Iniciando migración de usuarios y direcciones con Pandas...")

    # Transformar usuarios
    df_new_users = df_old_users.copy()
    df_new_users['registration_date'] = parse_date_string_pd(df_new_users['registration_date_str'], ["%Y-%m-%d %H:%M"])
    df_new_users['registration_date'] = df_new_users['registration_date'].fillna(
        datetime.utcnow())  # Default para no parseados
    df_new_users['hashed_password'] = "placeholder_password_needs_reset_pd"  # Placeholder
    df_new_users['is_active'] = True
    df_new_users['is_superuser'] = False

    # Parsear direcciones
    address_cols = df_new_users['address_combined'].apply(parse_combined_address_pd)
    df_new_users = pd.concat([df_new_users, address_cols], axis=1)

    users_migrated_count = 0
    addresses_migrated_count = 0

    # Insertar usuarios y luego direcciones
    # Podríamos usar df_new_users.to_sql si las transformaciones fueran más directas
    # o construir una lista de diccionarios para bulk_insert_mappings

    objects_to_add = []
    for index, row in df_new_users.iterrows():
        # Idempotencia: chequear si el usuario ya existe por username
        existing_user_id = new_db.query(User.id).filter(User.username == row['username']).scalar()
        if existing_user_id:
            map_old_user_id_to_new_user_id[row['id']] = existing_user_id
            map_new_user_username_to_new_id[row['username']] = existing_user_id
            # Asumir que la dirección también está, o tener lógica para manejarla
            existing_address_id = new_db.query(Address.id).filter(Address.user_id == existing_user_id,
                                                                  Address.is_default_shipping == True).scalar()
            if existing_address_id:
                map_new_user_id_to_default_address_id[existing_user_id] = existing_address_id
            continue

        user = User(
            username=row['username'],
            email=row['email'],
            full_name=row['full_name'],
            hashed_password=row['hashed_password'],
            is_active=row['is_active'],
            is_superuser=row['is_superuser'],
            registration_date=row['registration_date'].to_pydatetime() if pd.notna(
                row['registration_date']) else datetime.utcnow(),
            phone_number=row['phone_number_str']
        )
        # Añadir a la sesión para obtener el ID
        try:
            new_db.add(user)
            new_db.flush()
            map_old_user_id_to_new_user_id[row['id']] = user.id  # old_user.id es row['id']
            map_new_user_username_to_new_id[row['username']] = user.id
            users_migrated_count += 1

            address = Address(
                user_id=user.id,
                street=row['street'] or "N/A",
                city=row['city'] or "N/A",
                state=row['state'],  # Puede ser None
                zip_code=row['zip_code'] or "00000",
                country=row['country'] or "Unknown",
                is_default_shipping=True,
                is_default_billing=True
            )
            new_db.add(address)
            new_db.flush()
            map_new_user_id_to_default_address_id[user.id] = address.id
            addresses_migrated_count += 1

            if (users_migrated_count % BATCH_INSERT_SIZE == 0):
                new_db.commit()
                logger.info(f"Commit de lote de usuarios/direcciones. {users_migrated_count} usuarios migrados.")

        except IntegrityError:
            new_db.rollback()
            logger.error(f"Error de integridad con usuario {row['username']} o su dirección. Recuperando si existe...")
            existing_user_id = new_db.query(User.id).filter(User.username == row['username']).scalar()
            if existing_user_id:
                map_old_user_id_to_new_user_id[row['id']] = existing_user_id
                map_new_user_username_to_new_id[row['username']] = existing_user_id
                # ... manejar recuperación de dirección ...
        except Exception as e:
            new_db.rollback()
            logger.error(f"Error migrando usuario '{row['username']}' o su dirección: {e}")

    new_db.commit()  # Commit final
    logger.info(
        f"Migración de usuarios y direcciones (Pandas) finalizada. Usuarios: {users_migrated_count}, Direcciones: {addresses_migrated_count}.")
    # logger.debug(f"User map: {map_old_user_id_to_new_user_id}")


def migrate_products_pd(new_db: Session, df_old_products: pd.DataFrame):
    logger.info("Iniciando migración de productos con Pandas...")
    df_new_products = df_old_products.copy()

    df_new_products['price'] = parse_price_string_pd(df_new_products['price_str'])
    df_new_products['price'] = df_new_products['price'].fillna(Decimal('0.00'))

    df_new_products['created_at_dt'] = parse_date_string_pd(df_new_products['created_at_str'], ["%d/%m/%Y"])
    df_new_products['created_at_dt'] = df_new_products['created_at_dt'].fillna(datetime.utcnow())

    df_new_products['normalized_category_name'] = normalize_category_name_pd(df_new_products['category_name_redundant'])
    df_new_products['category_id'] = df_new_products['normalized_category_name'].map(
        map_old_category_name_to_new_category_id)
    # Alternativa si el mapeo anterior usó el nombre crudo:
    # df_new_products['category_id'] = df_new_products['category_name_redundant'].map(map_old_category_name_to_new_category_id)

    df_new_products['sku'] = "SKU-PD-" + df_new_products['id'].astype(str) + "-" + df_new_products[
        'product_name'].str.slice(0, 15).str.replace(' ', '-').str.upper()
    df_new_products['stock_quantity'] = np.random.randint(0, 100, size=len(df_new_products))

    missing_category_ids = df_new_products['category_id'].isna()
    if missing_category_ids.any():
        logger.warning(
            f"{missing_category_ids.sum()} productos no tienen category_id mapeado. Serán omitidos o necesitarán categoría por defecto.")
        # logger.warning(f"Categorías no encontradas: {df_new_products[missing_category_ids]['normalized_category_name'].unique()}")

    products_migrated_count = 0
    for index, row in df_new_products.iterrows():
        if pd.isna(row['category_id']):
            logger.warning(f"Producto '{row['product_name']}' (ID antiguo {row['id']}) no tiene category_id. Saltando.")
            continue

        # Idempotencia por SKU
        existing_product_id = new_db.query(Product.id).filter(Product.sku == row['sku']).scalar()
        if existing_product_id:
            map_old_product_id_to_new_product_id[row['id']] = existing_product_id
            map_old_product_name_to_new_product_id[row['product_name']] = existing_product_id
            continue

        product = Product(
            name=row['product_name'],
            description=row['description'],
            price=row['price'],
            sku=row['sku'],
            stock_quantity=int(row['stock_quantity']),
            category_id=int(row['category_id'])  # Asegurar que es int
        )
        product.created_at = row['created_at_dt'].to_pydatetime() if pd.notna(
            row['created_at_dt']) else datetime.utcnow()

        try:
            new_db.add(product)
            new_db.flush()
            map_old_product_id_to_new_product_id[row['id']] = product.id
            map_old_product_name_to_new_product_id[row['product_name']] = product.id
            products_migrated_count += 1
            if products_migrated_count % BATCH_INSERT_SIZE == 0:
                new_db.commit()
                logger.info(f"Commit de lote de productos. {products_migrated_count} productos migrados.")
        except IntegrityError:
            new_db.rollback()
            logger.error(
                f"Error de integridad con producto {row['product_name']} (SKU: {row['sku']}). Recuperando si existe...")
            existing_product_id = new_db.query(Product.id).filter(Product.sku == row['sku']).scalar()
            if existing_product_id:
                map_old_product_id_to_new_product_id[row['id']] = existing_product_id
                map_old_product_name_to_new_product_id[row['product_name']] = existing_product_id
        except Exception as e:
            new_db.rollback()
            logger.error(f"Error migrando producto '{row['product_name']}': {e}")

    new_db.commit()
    logger.info(f"Migración de productos (Pandas) finalizada. {products_migrated_count} productos creados.")


def migrate_orders_and_items_pd(new_db: Session, df_old_orders: pd.DataFrame):
    logger.info("Iniciando migración de pedidos e ítems con Pandas...")
    df_new_orders = df_old_orders.copy()

    df_new_orders['new_user_id'] = df_new_orders['user_identifier_text'].map(map_new_user_username_to_new_id)

    df_new_orders['order_date_dt'] = parse_date_string_pd(df_new_orders['order_date_str'],
                                                          ["%Y-%m-%d", "%m/%d/%Y %I:%M %p"])
    df_new_orders['order_date_dt'] = df_new_orders['order_date_dt'].fillna(datetime.utcnow())

    status_mapping_pd = {
        "pending": OrderStatusEnum.PENDING, "Pending": OrderStatusEnum.PENDING,
        "PROCESSING": OrderStatusEnum.PROCESSING,
        "shipped": OrderStatusEnum.SHIPPED, "Shipped": OrderStatusEnum.SHIPPED,
        "delivered": OrderStatusEnum.DELIVERED, "DELIVERED": OrderStatusEnum.DELIVERED,
        "cancelled": OrderStatusEnum.CANCELLED, "Returned?": OrderStatusEnum.REFUNDED,
    }
    df_new_orders['status_enum'] = df_new_orders['status_text'].map(status_mapping_pd).fillna(OrderStatusEnum.PENDING)

    df_new_orders['shipping_address_id'] = df_new_orders['new_user_id'].map(map_new_user_id_to_default_address_id)
    df_new_orders['billing_address_id'] = df_new_orders['shipping_address_id']  # Simplificación

    # Para OrderItems
    df_new_orders['new_product_id'] = df_new_orders['product_name_redundant'].map(
        map_old_product_name_to_new_product_id)
    df_new_orders['unit_price_decimal'] = parse_price_string_pd(df_new_orders['unit_price_str_redundant'])
    df_new_orders['unit_price_decimal'] = df_new_orders['unit_price_decimal'].fillna(Decimal('0.00'))
    df_new_orders['quantity_norm'] = df_new_orders['quantity'].fillna(1).astype(int)
    df_new_orders.loc[df_new_orders['quantity_norm'] < 1, 'quantity_norm'] = 1

    orders_migrated = 0
    items_migrated = 0

    # Filtrar pedidos que no pudieron ser mapeados (usuario, dirección, producto)
    df_new_orders.dropna(subset=['new_user_id', 'shipping_address_id', 'new_product_id'], inplace=True)
    logger.info(f"Después de filtrar por IDs faltantes, quedan {len(df_new_orders)} pedidos para procesar.")

    for index, row in df_new_orders.iterrows():
        order = Order(
            user_id=int(row['new_user_id']),
            order_date=row['order_date_dt'].to_pydatetime() if pd.notna(row['order_date_dt']) else datetime.utcnow(),
            status=row['status_enum'],
            shipping_address_id=int(row['shipping_address_id']),
            billing_address_id=int(row['billing_address_id'])
        )
        order.created_at = order.order_date  # Ajustar created_at al momento del pedido

        order_item = OrderItem(
            product_id=int(row['new_product_id']),
            quantity=int(row['quantity_norm']),
            unit_price_at_purchase=row['unit_price_decimal']
        )
        order.items.append(order_item)

        try:
            new_db.add(order)  # order_item se añade en cascada
            new_db.flush()
            orders_migrated += 1
            items_migrated += 1
            if orders_migrated % BATCH_INSERT_SIZE == 0:
                new_db.commit()
                logger.info(f"Commit de lote de pedidos. {orders_migrated} pedidos migrados.")
        except Exception as e:  # Captura más general aquí debido a la complejidad.
            new_db.rollback()
            logger.error(f"Error migrando pedido para usuario ID {row['new_user_id']} (antiguo ID {row['id']}): {e}")

    new_db.commit()
    logger.info(
        f"Migración de pedidos e ítems (Pandas) finalizada. Pedidos: {orders_migrated}, Ítems: {items_migrated}.")


def clear_new_database_tables_pd(new_db: Session):  # Misma función de limpieza que antes
    logger.info("Limpiando tablas de la nueva base de datos (versión Pandas)...")
    table_models = [OrderItem, Order, Address, Product, ProductCategory, User]  # Modelos directamente
    for model in table_models:
        try:
            table_name = model.__tablename__
            logger.info(f"Limpiando tabla {table_name}...")
            new_db.query(model).delete(synchronize_session=False)
            new_db.commit()
            logger.info(f"Tabla {table_name} limpiada.")
        except Exception as e:
            new_db.rollback()
            logger.error(f"Error limpiando tabla {table_name}: {e}")
            raise
    logger.info("Todas las tablas especificadas de la nueva BD han sido limpiadas (versión Pandas).")


# --- FUNCIÓN PRINCIPAL ---
def main_pandas(clear_target_db: bool = True):
    logger.info("--- INICIO DEL SCRIPT DE MIGRACIÓN DE DATOS (VERSIÓN PANDAS) ---")

    # 1. Leer datos antiguos a DataFrames
    logger.info("Leyendo datos de la base de datos antigua a DataFrames de Pandas...")
    try:
        df_old_users = pd.read_sql_table("old_users", old_db_engine)
        df_old_products = pd.read_sql_table("old_products", old_db_engine)
        df_old_orders = pd.read_sql_table("old_orders", old_db_engine)
        logger.info(
            f"Datos leídos: {len(df_old_users)} usuarios, {len(df_old_products)} productos, {len(df_old_orders)} pedidos.")
    except Exception as e:
        logger.critical(f"Error leyendo datos de la BD antigua a Pandas DataFrames: {e}")
        return

    new_db_session = NewSessionLocal()

    if clear_target_db:
        try:
            clear_new_database_tables_pd(new_db_session)
        except Exception as e:
            logger.critical(f"No se pudo limpiar la base de datos destino. Abortando. Error: {e}")
            new_db_session.close()
            return

    try:
        # 2. Migrar en orden, usando los DataFrames y mapas
        migrate_categories_pd(new_db_session, df_old_products)
        migrate_users_and_addresses_pd(new_db_session, df_old_users)
        migrate_products_pd(new_db_session, df_old_products)  # Usa mapa de categorías
        migrate_orders_and_items_pd(new_db_session, df_old_orders)  # Usa mapas de user, product, address

        logger.info("--- MIGRACIÓN DE DATOS (PANDAS) COMPLETADA EXITOSAMENTE ---")

    except Exception as e:
        logger.critical(f"Error crítico durante el proceso de migración con Pandas: {e}", exc_info=True)
        new_db_session.rollback()
        logger.error("--- MIGRACIÓN DE DATOS (PANDAS) FALLIDA ---")
    finally:
        new_db_session.close()
        logger.info("Conexión a la nueva base de datos cerrada.")


if __name__ == "__main__":
    # Asegúrate de que pandas está instalado: pip install pandas
    # Para ejecutar: python scripts/migrate_data_pandas.py
    main_pandas(clear_target_db=True)
