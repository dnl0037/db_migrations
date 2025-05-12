import logging
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

from sqlalchemy.orm import Session, joinedload
from sqlalchemy.exc import IntegrityError

from config import get_old_db, get_new_db

from models_old.old_models import OldUser, OldProduct, OldOrder

from models_new import User, Address, ProductCategory, Product, Order, OrderItem, OrderStatusEnum

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("migration.log"),  # Log a archivo
        logging.StreamHandler()  # Log a consola
    ]
)
logger = logging.getLogger(__name__)

BATCH_SIZE = 500

old_user_id_to_new_user_map = {}  # {old_user_id: new_user_object}
old_category_name_to_new_category_id_map = {}  # {old_category_name: new_category_id}
old_product_id_to_new_product_map = {}  # {old_product_id: new_product_object}
old_product_name_to_new_product_id_map = {}  # {old_product_name: new_product_id}


def parse_datetime_flexible(date_str: str, formats: list) -> datetime | None:
    """Intenta parsear un string de fecha con una lista de formatos posibles."""
    if not date_str:
        return None
    for fmt in formats:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except (ValueError, TypeError):
            continue
    logger.warning(f"No se pudo parsear la fecha: '{date_str}' con los formatos provistos.")
    return None


def normalize_category_name(name: str) -> str:
    """Normaliza nombres de categorías (ej. minúsculas, quita espacios extra)."""
    if not name:
        return "Unknown"  # O alguna categoría por defecto
    return name.strip().capitalize()


def extract_price_decimal(price_str: str) -> Decimal | None:
    """Extrae un valor Decimal de un string de precio, ignorando símbolos de moneda."""
    if not price_str:
        return None
    # Remover caracteres no numéricos excepto el punto decimal y el signo negativo
    # Esto es una simplificación. Una librería de parsing de moneda sería más robusta.
    cleaned_price_str = re.sub(r"[^0-9.-]", "", price_str)
    if not cleaned_price_str:  # Si después de limpiar no queda nada (ej. "contact us")
        logger.warning(f"No se pudo extraer un número del precio: '{price_str}'")
        return None
    try:
        # Tomar la primera parte si hay múltiples números (ej. "10.99-12.99")
        # Esto es una heurística y podría no ser siempre correcta.
        numeric_part = cleaned_price_str.split('-')[0]
        return Decimal(numeric_part)
    except InvalidOperation:
        logger.warning(f"Error al convertir a Decimal el precio: '{price_str}' (limpiado: '{cleaned_price_str}')")
        return None


def parse_address(address_combined: str) -> dict:
    """
    Intenta parsear una dirección combinada en componentes.
    Esta es una función MUY simplificada. En un caso real, usarías
    una librería como `pyap` o un servicio de geocodificación/parsing de direcciones.
    """
    parts = [p.strip() for p in address_combined.split(',')]
    parsed = {
        "street": "N/A", "city": "N/A", "state": "N/A",
        "zip_code": "N/A", "country": "N/A"
    }

    if not address_combined:
        return parsed

    # Heurística simple (esto fallará en muchos casos reales)
    parsed["street"] = parts[0] if len(parts) > 0 else "N/A"
    if len(parts) > 1:
        # Intentar extraer Zip Code y Estado (formato USA típico: City, ST ZIPCODE)
        city_state_zip_candidate = parts[-2 if len(parts) > 2 else -1]  # Penúltimo o último

        # Buscar ZIP (5 dígitos o 5-4)
        zip_match = re.search(r'\b\d{5}(?:-\d{4})?\b', city_state_zip_candidate)
        if zip_match:
            parsed["zip_code"] = zip_match.group(0)
            # Remover ZIP para aislar City, ST
            city_state_candidate = city_state_zip_candidate.replace(parsed["zip_code"], "").strip()
            # Buscar State Abbr (2 letras mayúsculas)
            state_match = re.search(r'\b([A-Z]{2})\b', city_state_candidate)
            if state_match:
                parsed["state"] = state_match.group(1)
                parsed["city"] = city_state_candidate.replace(parsed["state"], "").replace(",", "").strip()
            else:
                parsed["city"] = city_state_candidate.replace(",", "").strip()

        else:  # Si no hay ZIP, asumir que es la ciudad
            parsed["city"] = city_state_zip_candidate.replace(",", "").strip()

    parsed["country"] = parts[-1] if len(
        parts) > 1 and not zip_match else "USA"  # Asumir USA si no hay país explícito y hay ZIP

    # Limpieza final
    for key in ["street", "city", "state", "zip_code", "country"]:
        if not parsed[key] or len(parsed[key]) > 250:  # Límite de longitud para campos
            parsed[key] = "N/A" if key not in ["state"] else None

    if parsed["city"] == "N/A" and len(parts) > 1: parsed["city"] = parts[1]

    # Validaciones de longitud simples para evitar errores de BD
    parsed["street"] = parsed["street"][:254]
    parsed["city"] = parsed["city"][:99]
    parsed["state"] = parsed["state"][:99] if parsed["state"] else None
    parsed["zip_code"] = parsed["zip_code"][:19]
    parsed["country"] = parsed["country"][:99]

    return parsed


def map_order_status(old_status: str) -> OrderStatusEnum:
    """Mapea el estado de pedido antiguo al nuevo Enum."""
    if not old_status:
        return OrderStatusEnum.PENDING  # Por defecto

    s = old_status.lower().strip()
    if s in ["pending", "processing"]:  # Combinamos pending y processing
        return OrderStatusEnum.PENDING
    if s in ["shipped", "enviado"]:
        return OrderStatusEnum.SHIPPED
    if s in ["delivered", "entregado"]:
        return OrderStatusEnum.DELIVERED
    if s in ["cancelled", "cancelado"]:
        return OrderStatusEnum.CANCELLED
    # Podríamos añadir más mapeos o un log si no se reconoce
    logger.warning(f"Estado de pedido no reconocido: '{old_status}'. Usando PENDING por defecto.")
    return OrderStatusEnum.PENDING


# --- Funciones de Migración por Entidad ---

def clear_nwe_database_tables(db: Session):
    """Limpia todas las tablas en la nueva base de datos en el orden correcto."""
    logger.info("Limpiando tablas en la nueva base de datos...")
    # El orden es importante debido a las FKs. De hijos a padres.
    table_order = [OrderItem.__table__, Order.__table__, Address.__table__,
                   Product.__table__, ProductCategory.__table__, User.__table__]

    for table in table_order:
        try:
            logger.info(f"Limpiando tabla: {table.name}")
            db.execute(table.delete())
        except Exception as e:
            logger.error(f"Error limpiando tabla {table.name}: {e}")
            # Podríamos querer detenernos aquí o continuar con precaución
    db.commit()
    logger.info("Tablas de la nueva base de datos limpiadas.")


def migrate_product_categories(old_db: Session, new_db: Session):
    logger.info("Iniciando migración de categorías de productos...")
    processed_count = 0
    success_count = 0

    # Obtener todas las categorías distintas de la tabla antigua de productos
    # Esto puede ser ineficiente si hay muchísimos productos y pocas categorías.
    # Una alternativa sería procesar productos y añadir categorías sobre la marcha.
    # Pero para asegurar que todas las categorías existen antes de migrar productos:
    try:
        distinct_categories = old_db.query(OldProduct.category_name_redundant).distinct().all()
    except Exception as e:
        logger.error(f"Error al leer categorías de OldProduct: {e}")
        return

    logger.info(f"Se encontraron {len(distinct_categories)} nombres de categorías distintas en la BD antigua.")

    for cat_row in distinct_categories:
        old_cat_name = cat_row[0]
        if not old_cat_name:  # Ignorar si el nombre de la categoría es nulo o vacío
            logger.warning("Se encontró un nombre de categoría vacío/nulo, será ignorado.")
            continue

        normalized_name = normalize_category_name(old_cat_name)
        processed_count += 1

        # Evitar duplicados si ya se procesó una variante del nombre
        if normalized_name in old_category_name_to_new_category_id_map:
            logger.info(f"Categoría '{normalized_name}' (original: '{old_cat_name}') ya mapeada. Saltando.")
            continue

        # Crear nueva categoría
        try:
            # Comprobar si ya existe en la nueva BD (por si el script se corre varias veces sin limpiar)
            existing_category = new_db.query(ProductCategory).filter(ProductCategory.name == normalized_name).first()
            if existing_category:
                new_category_id = existing_category.id
                logger.info(f"Categoría '{normalized_name}' ya existe en la nueva BD con ID {new_category_id}.")
            else:
                new_category = ProductCategory(name=normalized_name, description=f"Categoría para {normalized_name}")
                new_db.add(new_category)
                new_db.flush()
                new_category_id = new_category.id
                logger.info(
                    f"Categoría '{normalized_name}' (original: '{old_cat_name}') creada con ID {new_category_id}.")

            old_category_name_to_new_category_id_map[normalized_name] = new_category_id
            # También mapear el nombre original si es diferente, para la búsqueda al migrar productos
            if old_cat_name != normalized_name:
                old_category_name_to_new_category_id_map[old_cat_name] = new_category_id
            success_count += 1

        except IntegrityError as e:
            new_db.rollback()
            logger.error(
                f"Error de integridad al crear categoría '{normalized_name}': {e}. Puede que ya exista si la normalización produce duplicados.")
            # Intentar cargarla si ya existe
            existing_category = new_db.query(ProductCategory).filter(ProductCategory.name == normalized_name).first()
            if existing_category:
                old_category_name_to_new_category_id_map[normalized_name] = existing_category.id
                if old_cat_name != normalized_name:
                    old_category_name_to_new_category_id_map[old_cat_name] = existing_category.id
                logger.info(f"Categoría '{normalized_name}' recuperada después de error de integridad.")
            else:
                logger.error(f"No se pudo recuperar la categoría '{normalized_name}' después del error.")
        except Exception as e:
            new_db.rollback()
            logger.error(f"Error general al procesar categoría '{normalized_name}': {e}")

        if processed_count % BATCH_SIZE == 0:  # Aunque aquí no es un batch real, sirve para el commit
            try:
                new_db.commit()
                logger.info(f"Commit de lote de categorías procesadas ({processed_count}).")
            except Exception as e:
                new_db.rollback()
                logger.error(f"Error en commit de lote de categorías: {e}")

    try:
        new_db.commit()  # Commit final para las restantes
    except Exception as e:
        new_db.rollback()
        logger.error(f"Error en commit final de categorías: {e}")

    logger.info(
        f"Migración de categorías de productos finalizada. Procesadas: {processed_count}, Creadas/Mapeadas con éxito: {success_count}.")


def migrate_users_and_addresses(old_db: Session, new_db: Session):
    logger.info("Iniciando migración de usuarios y direcciones...")
    total_old_users = old_db.query(OldUser).count()
    logger.info(f"Total de usuarios en la BD antigua: {total_old_users}")

    processed_users_count = 0
    successful_users_count = 0
    successful_addresses_count = 0

    # Usar yield_per para procesar en lotes sin cargar t_odo en memoria
    for old_user_batch_idx, old_user in enumerate(old_db.query(OldUser).yield_per(BATCH_SIZE)):
        processed_users_count += 1

        # --- Transformar Usuario ---
        reg_date_formats = ["%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]  # Añadir más si es necesario
        registration_date = parse_datetime_flexible(old_user.registration_date_str, reg_date_formats)
        if not registration_date:
            registration_date = datetime.now()  # Default si el parseo falla
            logger.warning(f"Usando fecha actual para registro de OldUser ID {old_user.id} debido a fallo de parseo.")

        # Para la contraseña, como no la tenemos, generamos una placeholder.
        # En un escenario real, esto requeriría un reseteo de contraseñas post-migración.
        hashed_password_placeholder = f"migrated_placeholder_for_{old_user.username}"

        try:
            new_user = User(
                username=old_user.username,
                email=old_user.email,
                full_name=old_user.full_name,
                hashed_password=hashed_password_placeholder,  # Usar un hasher real en prod (e.g., passlib)
                is_active=True,  # Asumir activo
                is_superuser=False,
                registration_date=registration_date,
                phone_number=old_user.phone_number_str
            )
            new_db.add(new_user)
            new_db.flush()  # Para obtener el new_user.id

            old_user_id_to_new_user_map[old_user.id] = new_user  # Guardar el objeto para fácil acceso
            successful_users_count += 1
            logger.debug(f"Usuario '{old_user.username}' (Old ID: {old_user.id}) migrado a New ID: {new_user.id}")

            # --- Transformar y Crear Dirección ---
            if old_user.address_combined:
                parsed_addr = parse_address(old_user.address_combined)

                # Simple validación para no insertar direcciones completamente vacías
                if any(v != "N/A" and v is not None for k, v in parsed_addr.items() if k != 'state'):
                    new_address = Address(
                        user_id=new_user.id,
                        street=parsed_addr["street"],
                        city=parsed_addr["city"],
                        state=parsed_addr["state"],
                        zip_code=parsed_addr["zip_code"],
                        country=parsed_addr["country"],
                        is_default_shipping=True,  # Asumir como default
                        is_default_billing=True  # Asumir como default
                    )
                    new_db.add(new_address)
                    successful_addresses_count += 1
                    logger.debug(f"Dirección para NewUser ID {new_user.id} migrada.")
                else:
                    logger.warning(
                        f"Dirección combinada para OldUser ID {old_user.id} ('{old_user.address_combined}') resultó en datos N/A, no se creó dirección.")
            else:
                logger.info(f"OldUser ID {old_user.id} no tiene dirección combinada. No se creará dirección.")

            # Commit por lotes
            if processed_users_count % BATCH_SIZE == 0:
                new_db.commit()
                logger.info(f"Commit de lote de usuarios/direcciones ({processed_users_count}/{total_old_users}).")

        except IntegrityError as e:
            new_db.rollback()
            logger.error(
                f"Error de integridad al migrar usuario Old ID {old_user.id} ('{old_user.username}'): {e}. Podría ser duplicado de username/email.")
            # Intentar recuperar el usuario si ya existe (ej. por ejecuciones previas del script)
            existing_user = new_db.query(User).filter(User.username == old_user.username).first()
            if existing_user:
                old_user_id_to_new_user_map[old_user.id] = existing_user
                logger.info(f"Usuario '{old_user.username}' recuperado después de error de integridad.")
            else:
                logger.error(f"No se pudo recuperar el usuario '{old_user.username}' después del error.")

        except Exception as e:
            new_db.rollback()
            logger.error(f"Error general al migrar usuario Old ID {old_user.id} ('{old_user.username}'): {e}")

    try:
        new_db.commit()  # Commit final para el último lote
    except Exception as e:
        new_db.rollback()
        logger.error(f"Error en commit final de usuarios/direcciones: {e}")

    logger.info(
        f"Migración de usuarios finalizada. Procesados: {processed_users_count}, Usuarios migrados con éxito: {successful_users_count}, Direcciones migradas con éxito: {successful_addresses_count}.")


def migrate_products(old_db: Session, new_db: Session):
    logger.info("Iniciando migración de productos...")
    total_old_products = old_db.query(OldProduct).count()
    logger.info(f"Total de productos en la BD antigua: {total_old_products}")

    processed_count = 0
    success_count = 0

    for old_product in old_db.query(OldProduct).yield_per(BATCH_SIZE):
        processed_count += 1

        # --- Transformar Producto ---
        price = extract_price_decimal(old_product.price_str)
        if price is None:
            logger.warning(
                f"Producto Old ID {old_product.id} ('{old_product.product_name}') tiene precio inválido ('{old_product.price_str}'). Usando 0.0 o saltando.")
            price = Decimal("0.00")  # O decidir omitir el producto

        date_formats = ["%d/%m/%Y", "%Y-%m-%d"]  # Añadir más formatos si es necesario
        created_at_old_product = parse_datetime_flexible(old_product.created_at_str, date_formats)
        # La nueva BD tiene created_at con default, así que no es crítico si falla el parseo.
        # Pero si quisiéramos preservar la fecha original, necesitamos el parseo.

        # Obtener new_category_id
        normalized_cat_name = normalize_category_name(old_product.category_name_redundant)
        new_category_id = old_category_name_to_new_category_id_map.get(normalized_cat_name)
        if not new_category_id:
            # Si la categoría no se encontró (quizás porque era nula o no se procesó bien)
            # podríamos asignarla a una categoría "Unknown" o registrar un error grave.
            unknown_cat_name = "Unknown"
            if unknown_cat_name not in old_category_name_to_new_category_id_map:
                # Crear categoría "Unknown" si no existe
                try:
                    logger.info(f"Creando categoría '{unknown_cat_name}' por defecto...")
                    default_cat = ProductCategory(name=unknown_cat_name,
                                                  description="Categoría por defecto para productos no categorizados.")
                    new_db.add(default_cat)
                    new_db.flush()
                    old_category_name_to_new_category_id_map[unknown_cat_name] = default_cat.id
                    new_category_id = default_cat.id
                    new_db.commit()  # Commit anidado, cuidado si la transacción externa falla
                except Exception as cat_err:
                    new_db.rollback()
                    logger.error(f"No se pudo crear categoría por defecto '{unknown_cat_name}': {cat_err}")
                    # Saltar este producto o manejar el error de otra forma
                    logger.error(
                        f"Producto Old ID {old_product.id} no tiene categoría válida ('{old_product.category_name_redundant}') y no se pudo asignar default. Saltando producto.")
                    continue
            else:
                new_category_id = old_category_name_to_new_category_id_map[unknown_cat_name]
            logger.warning(
                f"Producto Old ID {old_product.id} no tenía categoría válida ('{old_product.category_name_redundant}'). Asignado a '{unknown_cat_name}'.")

        # Generar SKU
        sku = f"SKU-{old_product.id:05d}-{random.randint(100, 999)}"  # Ejemplo de SKU

        try:
            new_product = Product(
                name=old_product.product_name,
                description=old_product.description,
                price=price,
                sku=sku,  # Asegurarse que sea único
                stock_quantity=random.randint(0, 100),  # Stock aleatorio ya que no existe en la antigua
                category_id=new_category_id
                # created_at se manejará por defecto por la BD si created_at_old_product es None
                # Si created_at_old_product tiene valor, y quieres usarlo, debes pasarlo explícitamente.
                # Y el modelo NewBase debería permitirlo (ej. no forzar server_default si se provee valor)
                # Para simplificar, dejaremos que created_at tome el valor actual.
            )
            if created_at_old_product:  # Si pudimos parsear la fecha antigua
                new_product.created_at = created_at_old_product  # Sobreescribir el default
                new_product.updated_at = created_at_old_product  # También updated_at para consistencia inicial

            new_db.add(new_product)
            new_db.flush()  # Para obtener new_product.id

            old_product_id_to_new_product_map[old_product.id] = new_product
            old_product_name_to_new_product_id_map[old_product.product_name] = new_product.id  # Para OldOrder
            success_count += 1
            logger.debug(
                f"Producto '{old_product.product_name}' (Old ID: {old_product.id}) migrado a New ID: {new_product.id}")

            if processed_count % BATCH_SIZE == 0:
                new_db.commit()
                logger.info(f"Commit de lote de productos ({processed_count}/{total_old_products}).")

        except IntegrityError as e:
            new_db.rollback()
            logger.error(
                f"Error de integridad al migrar producto Old ID {old_product.id} ('{old_product.product_name}'): {e}. Podría ser SKU duplicado.")
            # Si el SKU es el problema, se podría intentar regenerar.
            # O recuperar si ya existe.
            existing_product = new_db.query(Product).filter(
                Product.sku == sku).first()  # O por nombre si el SKU no es la causa
            if existing_product:
                old_product_id_to_new_product_map[old_product.id] = existing_product
                old_product_name_to_new_product_id_map[old_product.product_name] = existing_product.id
                logger.info(f"Producto '{old_product.product_name}' recuperado después de error de integridad.")
            else:
                logger.error(f"No se pudo recuperar el producto '{old_product.product_name}' después del error.")


        except Exception as e:
            new_db.rollback()
            logger.error(
                f"Error general al migrar producto Old ID {old_product.id} ('{old_product.product_name}'): {e}")

    try:
        new_db.commit()  # Commit final
    except Exception as e:
        new_db.rollback()
        logger.error(f"Error en commit final de productos: {e}")

    logger.info(f"Migración de productos finalizada. Procesados: {processed_count}, Exitosos: {success_count}.")


def migrate_orders_and_items(old_db: Session, new_db: Session):
    logger.info("Iniciando migración de pedidos y sus ítems...")
    total_old_orders = old_db.query(OldOrder).count()
    logger.info(f"Total de pedidos en la BD antigua: {total_old_orders}")

    processed_count = 0
    successful_orders_count = 0
    successful_items_count = 0

    # Cargar usuarios para búsqueda eficiente de user_identifier_text
    # Esto puede consumir memoria si hay muchísimos usuarios, pero para 10k es manejable.
    # Alternativa: consultar por cada pedido (más lento)
    new_users_by_username = {u.username: u for u in new_db.query(User).all()}
    # new_users_by_email = {u.email: u for u in new_users_by_username.values()} # Si emails son fiables

    for old_order in old_db.query(OldOrder).yield_per(BATCH_SIZE):
        processed_count += 1
        new_user_object = None

        # --- Buscar el Nuevo Usuario ---
        # Asumimos que user_identifier_text es principalmente el username
        user_identifier = old_order.user_identifier_text
        if user_identifier in new_users_by_username:
            new_user_object = new_users_by_username[user_identifier]
        # Podríamos añadir lógica para buscar por email si falla el username
        # elif user_identifier in new_users_by_email:
        # new_user_object = new_users_by_email[user_identifier]

        if not new_user_object:
            logger.warning(
                f"No se encontró usuario para OldOrder ID {old_order.id} (identificador: '{user_identifier}'). Saltando pedido.")
            continue

        # --- Obtener Dirección de Envío/Facturación ---
        # Usar la primera dirección (default) del usuario migrado.
        # En un escenario real, esto podría ser más complejo si hay múltiples direcciones.
        # Hacemos una query para la dirección del usuario si no la tenemos ya en new_user_object.addresses
        # O si el new_user_object no tiene la relación 'addresses' cargada.

        # Para este script, asumimos que el new_user_object que tenemos en old_user_id_to_new_user_map
        # es el objeto que creamos y al que podríamos haber asociado una dirección.
        # Mejoramos esto obteniendo el usuario mapeado previamente

        # Esta línea es incorrecta, user_identifier es el string, no el ID antiguo.
        # new_user_object_from_map = old_user_id_to_new_user_map.get(old_order.user_identifier_text) NO!
        # Necesitamos buscar el old_user.id si tuviéramos esa relación en OldOrder.
        # Como no la tenemos, dependemos de new_users_by_username.

        # Cargar direcciones para el new_user_object si no están ya cargadas.
        # Esto es para asegurar que podemos acceder a new_user_object.addresses.
        # Sin embargo, si el objeto vino del diccionario new_users_by_username,
        # es probable que las relaciones no estén cargadas.
        # Es más seguro volver a consultar el usuario con sus direcciones.

        db_user_with_addresses = new_db.query(User).options(joinedload(User.addresses)).filter(
            User.id == new_user_object.id).first()
        if not db_user_with_addresses or not db_user_with_addresses.addresses:
            logger.warning(
                f"Usuario New ID {new_user_object.id} no tiene direcciones asociadas. Saltando pedido Old ID {old_order.id}.")
            # O podríamos crear una dirección dummy aquí si la política lo permite.
            continue

        # Tomar la primera dirección como envío y facturación
        # Podríamos buscar la que esté marcada como is_default_shipping/billing
        shipping_address = next((addr for addr in db_user_with_addresses.addresses if addr.is_default_shipping), None)
        if not shipping_address:
            shipping_address = db_user_with_addresses.addresses[0]  # Tomar la primera si no hay default

        billing_address = next((addr for addr in db_user_with_addresses.addresses if addr.is_default_billing), None)
        if not billing_address:
            billing_address = shipping_address  # Usar la de envío si no hay de facturación específica

        # --- Transformar Pedido ---
        order_date_formats = ["%Y-%m-%d", "%m/%d/%Y %I:%M %p", "%Y-%m-%d %H:%M:%S"]
        order_date = parse_datetime_flexible(old_order.order_date_str, order_date_formats)
        if not order_date:
            order_date = datetime.now()  # Default
            logger.warning(f"Usando fecha actual para OldOrder ID {old_order.id} debido a fallo de parseo de fecha.")

        status = map_order_status(old_order.status_text)

        # total_order_amount_str -> No lo usaremos directamente, se calculará de los items si es necesario

        try:
            new_order = Order(
                user_id=new_user_object.id,
                order_date=order_date,
                status=status,
                shipping_address_id=shipping_address.id,
                billing_address_id=billing_address.id
                # created_at/updated_at serán manejados por defecto
            )
            new_db.add(new_order)
            new_db.flush()  # Para obtener new_order.id
            successful_orders_count += 1
            logger.debug(
                f"Pedido Old ID {old_order.id} migrado a New ID {new_order.id} para NewUser ID {new_user_object.id}")

            # --- Transformar y Crear OrderItem ---
            # Recordar que nuestro OldOrder tiene product_name_redundant, quantity, unit_price_str_redundant

            # Buscar el new_product_id
            new_product_id = old_product_name_to_new_product_id_map.get(old_order.product_name_redundant)
            if not new_product_id:
                # Si el producto no está en el mapeo por nombre, quizás buscar por ID si tuviéramos old_product_id
                # O registrar error y saltar item/pedido
                logger.error(
                    f"No se encontró producto '{old_order.product_name_redundant}' en el mapeo para OldOrder ID {old_order.id}. Saltando ítem.")
                # Podríamos decidir hacer rollback del pedido si un ítem es crucial.
                # Por ahora, el pedido se crea pero sin este ítem.
                # OJO: Esto puede dejar pedidos "vacíos". Considerar la lógica.
                # Si el producto es indispensable, se debe saltar el pedido entero o marcarlo.
                # Aquí, si el producto no se encuentra, el pedido se crea, pero sin este item.
                # Esto no es ideal. Mejor sería saltar el pedido si el producto es esencial.
                # Para esta simulación, lo dejamos así, pero en un caso real, esto necesita una decisión de negocio.
                # DECISIÓN: Si el producto no se encuentra, no crearemos el OrderItem.
                # Esto implica que un pedido puede quedar sin items si el producto no se mapeó.
                # Una mejor estrategia sería saltar el pedido completo.
                # Vamos a hacer esto: si no hay producto, no se crea el pedido.
                # Para ello, el new_db.add(new_order) y flush() deberían ir DESPUÉS de verificar el producto.
                # Por ahora, lo dejo así para ilustrar el problema, pero ya lo indiqué.
                # REFACTORING SUGERIDO: mover la creación del pedido después de validar el producto.
                # O, si el producto no existe, hacer new_db.expunge(new_order) si ya se añadió a la sesión
                # y no hacer commit de este pedido.
                # Para simplificar el ejemplo actual, si el producto no se encuentra, el OrderItem no se crea.

                # Aquí está el ajuste: si el producto no se encuentra, borramos el pedido de la sesión.
                new_db.expunge(new_order)  # Quitar de la sesión
                successful_orders_count -= 1  # Decrementar el contador
                logger.warning(
                    f"Pedido Old ID {old_order.id} no se migró porque el producto '{old_order.product_name_redundant}' no pudo ser mapeado.")
                if processed_count % BATCH_SIZE == 0:  # Si estamos en un commit de lote, necesitamos hacer rollback de este pedido
                    new_db.rollback()  # Revertir cualquier cosa de este pedido no commiteado aún
                continue  # Saltar al siguiente OldOrder

            unit_price = extract_price_decimal(old_order.unit_price_str_redundant)
            if unit_price is None:
                logger.warning(
                    f"Precio unitario inválido ('{old_order.unit_price_str_redundant}') para producto '{old_order.product_name_redundant}' en OldOrder ID {old_order.id}. Usando 0.0.")
                unit_price = Decimal("0.00")

            quantity = old_order.quantity if old_order.quantity and old_order.quantity > 0 else 1  # Asegurar cantidad positiva

            new_order_item = OrderItem(
                order_id=new_order.id,
                product_id=new_product_id,
                quantity=quantity,
                unit_price_at_purchase=unit_price
            )
            new_db.add(new_order_item)
            successful_items_count += 1
            logger.debug(
                f"OrderItem creado para NewOrder ID {new_order.id} (Producto: '{old_order.product_name_redundant}')")

            # Commit por lotes
            if processed_count % BATCH_SIZE == 0:
                new_db.commit()
                logger.info(f"Commit de lote de pedidos/ítems ({processed_count}/{total_old_orders}).")

        except IntegrityError as e:
            new_db.rollback()
            logger.error(f"Error de integridad al migrar OldOrder ID {old_order.id}: {e}")
        except Exception as e:
            new_db.rollback()
            logger.error(f"Error general al migrar OldOrder ID {old_order.id}: {e}")

    try:
        new_db.commit()  # Commit final
    except Exception as e:
        new_db.rollback()
        logger.error(f"Error en commit final de pedidos/ítems: {e}")

    logger.info(
        f"Migración de pedidos/ítems finalizada. Procesados: {processed_count}, Pedidos exitosos: {successful_orders_count}, Ítems exitosos: {successful_items_count}.")


# --- Función Principal de Migración ---
import random  # Necesario para el SKU y stock_quantity en migrate_products


def main_migration_process():
    logger.info("========= INICIO DEL PROCESO DE MIGRACIÓN DE DATOS =========")

    old_db_session_gen = get_old_db()
    new_db_session_gen = get_new_db()

    old_db = next(old_db_session_gen)
    new_db = next(new_db_session_gen)

    try:
        # --- PASO 0: Limpiar tablas de la nueva BD (opcional, para ejecuciones de prueba) ---
        # En un entorno de producción, esto se manejaría con más cuidado.
        # Preguntar al usuario o usar un flag para esto.
        # clear_new_database_tables(new_db) # Descomentar si se quiere limpiar antes de cada ejecución

        # --- PASO 1: Migrar ProductCategories ---
        # Las categorías se derivan de los productos antiguos.
        migrate_product_categories(old_db, new_db)
        logger.info(f"Mapeo de categorías actual: {len(old_category_name_to_new_category_id_map)} entradas.")

        # --- PASO 2: Migrar Users y sus Addresses ---
        migrate_users_and_addresses(old_db, new_db)
        logger.info(f"Mapeo de usuarios actual: {len(old_user_id_to_new_user_map)} entradas.")

        # --- PASO 3: Migrar Products ---
        # Depende de ProductCategories
        migrate_products(old_db, new_db)
        logger.info(f"Mapeo de productos (ID) actual: {len(old_product_id_to_new_product_map)} entradas.")
        logger.info(f"Mapeo de productos (nombre) actual: {len(old_product_name_to_new_product_id_map)} entradas.")

        # --- PASO 4: Migrar Orders y OrderItems ---
        # Depende de Users, Addresses, Products
        migrate_orders_and_items(old_db, new_db)

        logger.info("Todas las etapas de migración completadas.")

    except Exception as e:
        logger.critical(f"Error CRÍTICO durante el proceso de migración principal: {e}", exc_info=True)
        # En caso de un error no capturado en las funciones específicas, hacer rollback en la nueva DB.
        new_db.rollback()
    finally:
        logger.info("Cerrando sesiones de base de datos.")
        old_db.close()
        new_db.close()
        logger.info("========= FIN DEL PROCESO DE MIGRACIÓN DE DATOS =========")


if __name__ == "__main__":
    # Asegúrate de que las bases de datos 'old_bad_db' y 'new_good_db' existan y sean accesibles.
    # 'old_bad_db' debe estar poblada (usar populate_old_db.py).
    # 'new_good_db' debe tener las tablas creadas por Alembic.

    # Para una ejecución limpia, especialmente durante el desarrollo:
    # 1. (Opcional) Borra el archivo migration.log
    # 2. (Opcional, pero recomendado para pruebas) En psql o DBeaver, conéctate a 'new_good_db'
    #    y ejecuta:
    #    DELETE FROM order_items; DELETE FROM orders; DELETE FROM addresses;
    #    DELETE FROM products; DELETE FROM product_categories; DELETE FROM users;
    #    (O usa la función clear_new_database_tables descomentándola en main_migration_process)

    main_migration_process()
