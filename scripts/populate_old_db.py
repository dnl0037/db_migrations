import random
import logging
from faker import Faker
from sqlalchemy.orm import Session
from config import old_engine, OldBase, get_old_db
from models_old.old_models import OldUser, OldProduct, OldOrder

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

fake = Faker()  # Puedes usar localizaciones: Faker(['it_IT', 'en_US', 'ja_JP'])

# --- Constantes para la generación de datos ---
NUM_USERS = 2000  # Reducido para pruebas iniciales, subiremos a 10k+
NUM_PRODUCTS = 500  # Reducido para pruebas iniciales
NUM_ORDERS_PER_USER_AVG = 5  # Promedio de pedidos por usuario

# Listas para mantener IDs y datos que podrían ser referenciados (mal)
# No necesitamos IDs si no hay FKs, pero sí datos para "referenciar"
created_usernames = []
created_product_names_prices = []  # Lista de tuplas (name, price_str)

# Posibles estados de pedido (mal diseñados)
BAD_ORDER_STATUSES = ["pending", "Pending", "PROCESSING", "shipped", "Shipped", "delivered", "DELIVERED", "cancelled",
                      "Returned?"]
BAD_CATEGORIES = ["Electronics", "Books", "Home Goods", "Apparel", "Sports", "electronics", "libros", " ropa "]


def create_tables():
    """Crea todas las tablas definidas en OldBase en la base de datos antigua."""
    logger.info("Intentando crear tablas en la base de datos antigua...")
    try:
        OldBase.metadata.create_all(bind=old_engine)
        logger.info("Tablas creadas exitosamente (o ya existían).")
    except Exception as e:
        logger.error(f"Error al crear tablas: {e}")
        raise


def populate_users(db: Session, num_users: int):
    """Puebla la tabla OldUser con datos falsos."""
    logger.info(f"Poblando {num_users} usuarios antiguos...")
    users_to_add = []
    for i in range(num_users):
        profile = fake.profile()
        username = profile['username'] + str(random.randint(1, 1000))  # Asegurar unicidad

        # MALA PRÁCTICA: Fecha como string
        reg_date = fake.date_time_this_decade()
        registration_date_str = reg_date.strftime("%Y-%m-%d %H:%M")  # Un formato común pero aún string

        # MALA PRÁCTICA: Dirección combinada
        address_combined = f"{fake.street_address()}, {fake.city()}, {fake.state_abbr()} {fake.zipcode()}, {fake.country()}"

        user = OldUser(
            username=username,
            email=f"{username}@{fake.free_email_domain()}",  # Email basado en username para consistencia
            full_name=profile['name'],
            registration_date_str=registration_date_str,
            address_combined=address_combined,
            phone_number_str=fake.phone_number()[:20]
        )
        users_to_add.append(user)
        created_usernames.append(username)  # Guardar para "referenciar" en pedidos
        if (i + 1) % 1000 == 0:
            logger.info(f"Generados {i + 1}/{num_users} usuarios.")

    try:
        db.add_all(users_to_add)
        db.commit()
        logger.info(f"Poblados {len(users_to_add)} usuarios exitosamente.")
    except Exception as e:
        db.rollback()
        logger.error(f"Error al poblar usuarios: {e}")
        # Podríamos intentar insertar uno por uno si hay un error de duplicado masivo,
        # pero para la simulación, un fallo es suficiente para detener esta parte.


def populate_products(db: Session, num_products: int):
    """Puebla la tabla OldProduct con datos falsos."""
    logger.info(f"Poblando {num_products} productos antiguos...")
    products_to_add = []
    for i in range(num_products):
        product_name = f"{fake.word().capitalize()} {fake.word().capitalize()} {random.choice(['Device', 'Tool', 'Book', 'Gadget', 'Appliance'])}"

        # MALA PRÁCTICA: Precio como string, a veces con moneda
        price = round(random.uniform(5.0, 1000.0), 2)
        price_str = f"{price:.2f} {random.choice(['USD', 'EUR', '']) if random.random() < 0.3 else ''}".strip()

        # MALA PRÁCTICA: Fecha como string
        created_at = fake.date_time_this_year()
        created_at_str = created_at.strftime("%d/%m/%Y")  # Otro formato de fecha como string

        product = OldProduct(
            product_name=product_name,
            description=fake.sentence(nb_words=20),
            price_str=price_str,
            category_name_redundant=random.choice(BAD_CATEGORIES),
            created_at_str=created_at_str
        )
        products_to_add.append(product)
        created_product_names_prices.append((product_name, price_str))  # Guardar para "referenciar"
        if (i + 1) % 500 == 0:
            logger.info(f"Generados {i + 1}/{num_products} productos.")

    try:
        db.add_all(products_to_add)
        db.commit()
        logger.info(f"Poblados {len(products_to_add)} productos exitosamente.")
    except Exception as e:
        db.rollback()
        logger.error(f"Error al poblar productos: {e}")


def populate_orders(db: Session, num_orders_target: int):
    """
    Puebla la tabla OldOrder con datos falsos.
    El número de pedidos será variable por usuario.
    """
    logger.info(f"Intentando poblar aproximadamente {num_orders_target} pedidos antiguos...")
    if not created_usernames or not created_product_names_prices:
        logger.error("Se necesitan usuarios y productos creados para generar pedidos.")
        return

    orders_to_add = []
    order_count = 0
    # Generaremos pedidos hasta alcanzar o superar el objetivo
    # Esto es más realista que un número fijo por usuario, pero podría ser más lento
    # Para un control más estricto, iteraríamos sobre usuarios y luego un número aleatorio de pedidos por usuario.
    # Vamos a hacerlo por usuario para tener un control más predecible de la cantidad final.

    for username in created_usernames:
        num_orders_for_this_user = random.randint(1, NUM_ORDERS_PER_USER_AVG * 2)  # 1 a 2*avg pedidos por usuario
        for _ in range(num_orders_for_this_user):
            if order_count >= num_orders_target * 1.2:  # Cortar si ya generamos muchos más de lo esperado
                break

            # MALA PRÁCTICA: user_identifier_text podría ser username o a veces email
            user_identifier_text = username
            if random.random() < 0.1:  # 10% de las veces, usar email (si lo tenemos)
                # Para este script, siempre usaremos username para simplificar,
                # pero en un caso real podría ser más caótico.
                # user_email_obj = db.query(OldUser.email).filter(OldUser.username == username).first()
                # if user_email_obj: user_identifier_text = user_email_obj.email
                pass

            # MALA PRÁCTICA: Fecha de pedido como string, formato variado
            order_date = fake.date_time_between(start_date="-2y", end_date="now")
            if random.random() < 0.5:
                order_date_str = order_date.strftime("%Y-%m-%d")
            else:
                order_date_str = order_date.strftime("%m/%d/%Y %I:%M %p")

            # MALA PRÁCTICA: Datos de producto redundantes
            product_info = random.choice(created_product_names_prices)
            product_name_redundant = product_info[0]
            unit_price_str_redundant = product_info[1]  # Este es el precio string original, ej "20.00 USD"

            quantity = random.randint(1, 5)

            # MALA PRÁCTICA: Calcular total pero almacenarlo como string
            # Intentar extraer el número del precio_str (esto es parte del problema de los datos malos)
            try:
                price_numeric_part = "".join(
                    filter(lambda x: x.isdigit() or x == '.', unit_price_str_redundant.split(" ")[0]))
                unit_price_float = float(price_numeric_part)
                total_amount = round(unit_price_float * quantity, 2)
                total_order_amount_str = f"{total_amount:.2f}"  # A veces con moneda, a veces no
                if "USD" in unit_price_str_redundant:
                    total_order_amount_str += " USD"
                elif "EUR" in unit_price_str_redundant:
                    total_order_amount_str += " EUR"
            except ValueError:  # Si el precio_str es muy malo (ej. "contact us")
                total_order_amount_str = "N/A"  # O algún otro valor inválido

            order = OldOrder(
                user_identifier_text=user_identifier_text,
                order_date_str=order_date_str,
                status_text=random.choice(BAD_ORDER_STATUSES),
                product_name_redundant=product_name_redundant,
                quantity=quantity,
                unit_price_str_redundant=unit_price_str_redundant,
                total_order_amount_str=total_order_amount_str
            )
            orders_to_add.append(order)
            order_count += 1
            if order_count % 1000 == 0:
                logger.info(f"Generados {order_count} pedidos.")
        if order_count >= num_orders_target * 1.2:
            break

    try:
        db.add_all(orders_to_add)
        db.commit()
        logger.info(f"Poblados {len(orders_to_add)} pedidos exitosamente.")
    except Exception as e:
        db.rollback()
        logger.error(f"Error al poblar pedidos: {e}")


def main():
    logger.info("Iniciando proceso de creación y población de la base de datos ANTIGUADA.")

    # Crear tablas
    create_tables()

    # Obtener sesión de base de datos
    db_session_gen = get_old_db()
    db = next(db_session_gen)

    try:
        # Definir cantidades objetivo (ajusta a 10k para la simulación real, usa menos para pruebas rápidas)
        target_users = NUM_USERS
        target_products = NUM_PRODUCTS
        # target_orders será aproximadamente target_users * NUM_ORDERS_PER_USER_AVG
        target_total_orders = target_users * NUM_ORDERS_PER_USER_AVG

        # Poblar tablas
        # Comprueba si ya hay datos para no duplicar masivamente en ejecuciones repetidas (simple check)
        if db.query(OldUser).count() < target_users // 2:  # Si hay menos de la mitad de lo esperado
            populate_users(db, num_users=target_users)
        else:
            logger.info("Tabla de usuarios ya parece poblada. Saltando población de usuarios.")
            # Recargar created_usernames si los usuarios ya existen, para los pedidos
            # Esto es una simplificación; una mejor verificación contaría con más detalle o usaría flags.
            existing_users = db.query(OldUser.username).limit(target_users).all()
            created_usernames.extend([u.username for u in existing_users])

        if db.query(OldProduct).count() < target_products // 2:
            populate_products(db, num_products=target_products)
        else:
            logger.info("Tabla de productos ya parece poblada. Saltando población de productos.")
            existing_products = db.query(OldProduct.product_name, OldProduct.price_str).limit(target_products).all()
            created_product_names_prices.extend([(p.product_name, p.price_str) for p in existing_products])

        if db.query(OldOrder).count() < (
                target_total_orders // 2) and created_usernames and created_product_names_prices:
            populate_orders(db, num_orders_target=target_total_orders)
        elif not created_usernames or not created_product_names_prices:
            logger.warning("No se pudieron cargar usuarios o productos existentes para generar pedidos.")
        else:
            logger.info("Tabla de pedidos ya parece poblada. Saltando población de pedidos.")

        logger.info("Proceso de población finalizado.")

    finally:
        db.close()
        logger.info("Sesión de base de datos cerrada.")


if __name__ == "__main__":
    # Antes de ejecutar, asegúrate de que la base de datos 'old_bad_db' exista en PostgreSQL
    # y que las credenciales en .env sean correctas.
    # También puedes ejecutar `python config.py` para probar la conexión.
    main()
