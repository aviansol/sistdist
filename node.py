import socket
import threading
import time
import json
import random
from pymongo import MongoClient

# =====================
# Configuraciones
# =====================

lista_sucursales = ["CDMX", "GDL", "MTY", "SLP", "PUE", "QRO", "TOL", "VER", "OAX", "CHI"]

lista_puertos = [ x for x in range(60000, 60010)]

PUERTO_BROADCAST = 50000
PUERTO_NODO = random.choice(lista_puertos)
INTERVALO_DISCOVERY = 5
INTERVALO_VERIFICACION_MAESTRO = 10
NODOS_DESCUBIERTOS = {} # {ip: {"puerto": int, "sucursal": str}}
SOY_MAESTRO = False
def get_local_ip():
    try:
        # Se conecta a una dirección externa pero no envía datos
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip_local = s.getsockname()[0]
        s.close()
        return ip_local
    except Exception as e:
        print(f"Error al obtener la IP: {e}")
        return None

IP_LOCAL = get_local_ip()
MAESTRO_ACTUAL = None

OPERACION_ACTUAL = 0 # empezamos el contador en 0 en cada nodo
OPERACION_TIMESTAMP = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())


SUCURSAL = random.choice(lista_sucursales) + " " + str(random.randint(1, 10))

# =====================
# Conexión a MongoDB local
# =====================
mongo = MongoClient("mongodb://localhost:27017/")
db = mongo["sistema_distribuido"]
coleccion_inventario = db["inventario"]
coleccion_clientes = db["clientes"]
coleccion_guias = db["guias"]


# =====================
# Lectura de datos
# =====================

with open("mongo.json","r") as archivo:
    datos = json.load(archivo)

# checamos si la coleccion existe, si no la creamos
if "inventario" not in db.list_collection_names():
    db.create_collection("inventario")
    print("[INFO] Colección 'inventario' creada.")
    db.inventario.insert_many(datos["inventario"])
    print("Inventario actualizado")
if "clientes" not in db.list_collection_names():
    db.create_collection("clientes")
    print("[INFO] Colección 'clientes' creada.")
    db.clientes.insert_many(datos["clientes"])
    print("Clientes actualizados")
if "guias" not in db.list_collection_names():
    db.create_collection("guias")
    print("[INFO] Colección 'guias' creada.")
    db.guias.insert_many(datos["guias"])
    print("Guias actualizadas")

# =====================
# Registro local de logs
# =====================
LOG_FILE = f"log_{IP_LOCAL.replace('.', '_')}.txt"

def log_local(mensaje):
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"[{timestamp}] {mensaje}")

# =====================
# Inventario / Clientes en memoria
# =====================
inventario = {}  # {(id_articulo, serie): {"nombre": str, "cantidad": int, "ubicacion": str}}
clientes = {}    # {id_cliente: nombre}
guias = {}       # {codigo_guia: {"fecha_envio": str, "estado": str}}
lock_inventario = threading.Lock()

for item in coleccion_inventario.find():
    clave = (str(item["id"]), str(item["serie"]))
    inventario[clave] = {
        "id": item["id"],
        "serie": item["serie"],
        "nombre": item["nombre"],
        "cantidad": item["cantidad"],
        "ubicacion": item["ubicacion"],
    }

for cliente in coleccion_clientes.find():
    clientes[str(cliente["id"])] = {
        "id": cliente["id"],
        "nombre": cliente["nombre"],
        "email": cliente["email"],
        "telefono": cliente["telefono"]
    }

for guia in coleccion_guias.find():
    guias[str(guia["id"])] = {
        "codigo": guia["codigo"],
        "fecha_envio": guia["fecha_envio"],
        "estado": guia["estado"]
    }

# =====================
# Broadcast discovery
# =====================
def enviar_broadcast():
    global IP_LOCAL, PUERTO_NODO, SUCURSAL
    tiempo_vida = 10  # segundos
    start_time = time.time()
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        s.settimeout(1)
        while time.time() - start_time < tiempo_vida:
            mensaje = f"DISCOVER:{IP_LOCAL}:{PUERTO_NODO}:{SUCURSAL}"
            s.sendto(mensaje.encode(), ('<broadcast>', PUERTO_BROADCAST))
            time.sleep(1)
    print("[INFO] Broadcast discovery enviado")

# =====================
# Escuchar broadcast discovery (revive por 10s tras recibir)
# =====================
def escuchar_broadcast():
    global NODOS_DESCUBIERTOS, MAESTRO_ACTUAL, SOY_MAESTRO
    tiempo_vida = 10  # segundos
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            s.bind(('', PUERTO_BROADCAST))
            s.settimeout(tiempo_vida)
            start_time = time.time()
            while time.time() - start_time < tiempo_vida:
                try:
                    data, addr = s.recvfrom(1024)
                    mensaje = data.decode()
                    if mensaje.startswith("DISCOVER:"):
                        partes = mensaje.split(":")
                        ip_nodo = partes[1]
                        puerto_nodo = int(partes[2])
                        sucursal_nodo = partes[3] if len(partes) > 3 else "N/A"
                        if ip_nodo != IP_LOCAL:
                            NODOS_DESCUBIERTOS[ip_nodo] = {
                                "puerto": puerto_nodo,
                                "sucursal": sucursal_nodo
                            }
                            # Reiniciar el tiempo de vida al recibir un nuevo broadcast
                            start_time = time.time()
                            # Decidir maestro por IP mayor
                            ips = list(NODOS_DESCUBIERTOS.keys())
                            ips.append(IP_LOCAL)
                            nuevo_maestro = max(ips)
                            if IP_LOCAL == nuevo_maestro:
                                SOY_MAESTRO = True
                                MAESTRO_ACTUAL = IP_LOCAL
                                print(f"[INFO] Soy el nuevo nodo maestro (Sucursal: {SUCURSAL})")
                                notificar_nuevo_maestro()
                            else:
                                SOY_MAESTRO = False
                                MAESTRO_ACTUAL = nuevo_maestro
                                print(f"[INFO] Nodo esclavo. Maestro: {MAESTRO_ACTUAL}")
                except socket.timeout:
                    break
                except Exception:
                    continue
        break
    print("[INFO] Descubierto por: ", [x["sucursal"] for x in NODOS_DESCUBIERTOS.values()])

    

# =====================
# Elección de maestro (Bully)
# =====================
def elegir_maestro():
    global SOY_MAESTRO, MAESTRO_ACTUAL
    todos = list(NODOS_DESCUBIERTOS.keys()) + [IP_LOCAL]
    nuevo_maestro = max(todos)
    if IP_LOCAL == nuevo_maestro:
        SOY_MAESTRO = True
        MAESTRO_ACTUAL = IP_LOCAL
        print("[INFO] Soy el nuevo nodo maestro")
        notificar_nuevo_maestro()
    else:
        SOY_MAESTRO = False
        MAESTRO_ACTUAL = nuevo_maestro
        print("[INFO] Nodo esclavo. Maestro:", MAESTRO_ACTUAL)

# =====================
# Verificar salud del maestro
# =====================
def verificar_maestro():
    global MAESTRO_ACTUAL
    while True:
        time.sleep(INTERVALO_VERIFICACION_MAESTRO)
        if not SOY_MAESTRO and MAESTRO_ACTUAL:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(5)
                    s.connect((MAESTRO_ACTUAL, NODOS_DESCUBIERTOS[MAESTRO_ACTUAL]["puerto"]))
                    s.sendall(json.dumps({"tipo": "ping"}).encode())
                    s.recv(1024)
            except:
                print("[ALERTA] Maestro no responde. Iniciando nueva elección...")
                elegir_maestro()

articulos_en_uso = set()

# =====================
# Servidor TCP por nodo
# =====================
def servidor_nodo():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((IP_LOCAL, PUERTO_NODO))
        s.listen()
        print(f"[TCP] Escuchando en {IP_LOCAL}:{PUERTO_NODO}")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=atender_conexion, args=(conn, addr), daemon=True).start()

def atender_conexion(conn, addr):
    global OPERACION_ACTUAL, OPERACION_TIMESTAMP, SOY_MAESTRO, MAESTRO_ACTUAL, NODOS_DESCUBIERTOS, inventario, coleccion_inventario, coleccion_clientes, coleccion_guias, guias, clientes
    with conn:
        data = conn.recv(8192)
        if not data:
            return
        mensaje = json.loads(data.decode())
        tipo = mensaje.get("tipo")

        if tipo is None:
            print("[ERROR] Mensaje sin tipo recibido")
            conn.sendall(b"error")
            return
        else:
            OPERACION_ACTUAL += 1
            OPERACION_TIMESTAMP = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())

        if tipo == "ping":
            conn.sendall(b"pong")

        elif tipo == "compra":
            serie_art = mensaje["serie_articulo"]  # Nuevo: incluir número de serie
            id_cli = mensaje["id_cliente"]
            fecha = mensaje["fecha_envio"]
            id_art = mensaje["id_articulo"]
            origen = mensaje.get("origen", [IP_LOCAL, PUERTO_NODO])
            sucursal_remota = NODOS_DESCUBIERTOS.get(origen[0], {}).get("sucursal", SUCURSAL)

            with lock_inventario:
                # existe
                if (id_art, serie_art) not in inventario:
                    print(f"[ERROR] Artículo {id_art} (Serie: {serie_art}) no encontrado en el inventario local.")
                    conn.sendall(b"error")
                    return
                
                # reducimos la cantidad
                inventario[(id_art, serie_art)]['cantidad'] -= 1
                if inventario[(id_art, serie_art)]['cantidad'] <= 0:
                    print(f"[ERROR] No hay suficiente inventario para el artículo {id_art} (Serie: {serie_art}).")
                    conn.sendall(b"error")
                    return
                
                coleccion_inventario.find_one_and_update(
                    {"id": int(id_art), "serie": int(serie_art)},
                    {"$set": {
                        "cantidad": int(inventario[(id_art, serie_art)]['cantidad']),
                        # "ubicacion": SUCURSAL,
                        "nombre": inventario[(id_art, serie_art)]['nombre']},
                    },
                    upsert=True
                )
                log_local(f"Compra realizada: (Serie: {serie_art}) por cliente {id_cli} en {fecha}")
                print(f"[COMPRA] Artículo (Serie: {serie_art}) vendido a cliente {id_cli} en {fecha}")
                conn.sendall(b"ok")  # Al final del bloque de compra
                # actualizamos los registros en todos los nodos
                enviar_a_todos({
                    "tipo": "compra_realizada",
                    "origen": [IP_LOCAL, PUERTO_NODO],
                    "id_articulo": id_art,
                    "serie_articulo": serie_art,
                    "id_cliente": id_cli,
                    "fecha_envio": fecha,
                    "ubicacion": SUCURSAL
                })

            # Registrar guía de envío
            guia_codigo = f"{id_art}-{serie_art}-{inventario[(id_art, serie_art)]['ubicacion']}-{id_cli}"
            coleccion_guias.insert_one({
                "id": len(guias),
                "codigo": guia_codigo,
                "fecha_envio": fecha,
                "estado": "Enviado"
            })
            guias[len(guias)] = {
                "codigo": guia_codigo,
                "fecha_envio": fecha,
                "estado": "Enviado"
            }
            nombre_objeto = inventario[(id_art, serie_art)]["nombre"]
            print(f"[GUIA] Guía {guia_codigo} generada para la compra del artículo {nombre_objeto} (Serie: {serie_art}) por el cliente {id_cli}")
            conn.sendall(b"ok")  # Al final del bloque de compra

        elif tipo == "solicitar_compra":
            if not SOY_MAESTRO:
                return

            origen = mensaje.get("origen")
            ip_origen, puerto_origen = origen
            id_art = mensaje["id_articulo"]
            id_cli = mensaje["id_cliente"]
            serie_art = mensaje.get("serie_articulo", "N/A")
            fecha = mensaje.get("fecha_envio", time.strftime("%Y-%m-%d %H:%M:%S"))
            ubicacion_remota = NODOS_DESCUBIERTOS.get(ip_origen, {}).get("sucursal", SUCURSAL)

            # Verificar si el artículo ya está en uso
            if (id_art, serie_art) in articulos_en_uso:
                log_local(f"Rechazada compra por concurrencia: {id_art} (Serie: {serie_art})")
                print(f"[MAESTRO] Rechazada compra del artículo {id_art} (Serie: {serie_art}) (en uso)")
                return

            articulos_en_uso.add((id_art, serie_art))

            # Buscar qué nodo tiene el artículo
            nodo_con_articulo = None
            for ip, info in NODOS_DESCUBIERTOS.items():
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.settimeout(3)
                        s.connect((ip, info["puerto"]))
                        s.sendall(json.dumps({"tipo": "solicitar_estado"}).encode())
                        data = s.recv(8192)
                        estado = json.loads(data.decode())
                        inventario_nodo = { (str(item["id"]), str(item["serie"])): item for item in estado["inventario"] }
                        if (str(id_art), str(serie_art)) in inventario_nodo:
                            nodo_con_articulo = (ip, info["puerto"])
                            print(f"[INFO] Nodo {ip} tiene el artículo {id_art} (Serie: {serie_art})")
                            break
                except Exception as e:
                    print(f"[WARN] No se pudo consultar el nodo {ip}: {e}")
                if nodo_con_articulo:
                    break

            if not nodo_con_articulo:
                print(f"[MAESTRO] Ningún nodo tiene el artículo {id_art} (Serie: {serie_art}). Me lo adjudico.")
                with lock_inventario:
                    cantidad_anterior = inventario.get((id_art, serie_art), {}).get("cantidad", 0)
                    nombre_anterior = inventario.get((id_art, serie_art), {}).get("nombre")
                    try:
                        coleccion_inventario.find_one_and_replace(
                            {"id": int(id_art), "serie": int(serie_art)},
                            {
                                "id": int(id_art),
                                "serie": int(serie_art),
                                "nombre": nombre_anterior,
                                "cantidad": int(cantidad_anterior),
                                "ubicacion": SUCURSAL
                            },
                            upsert=True
                        )
                    except Exception as e:
                        coleccion_inventario.insert_one({
                            "id": int(id_art),
                            "serie": int(serie_art),
                            "nombre": nombre_anterior,
                            "cantidad": int(cantidad_anterior),
                            "ubicacion": SUCURSAL
                        })
                    inventario[(id_art, serie_art)] = {
                        "id": id_art,
                        "serie": serie_art,
                        "nombre": nombre_anterior,
                        "cantidad": cantidad_anterior,
                        "ubicacion": SUCURSAL
                    }

                    # ahora actualizamos el registro en todos
                    enviar_a_todos({
                        "tipo": "articulo_agregado",
                        "origen": [IP_LOCAL, PUERTO_NODO],  # para replicar el origen de la adición
                        "id_articulo": id_art,
                        "serie_articulo": serie_art,
                        "nombre_articulo": nombre_anterior,
                        "cantidad": cantidad_anterior,
                        "fecha_envio": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "ubicacion": SUCURSAL
                    })

                nodo_con_articulo = (IP_LOCAL, PUERTO_NODO)
        
            # Enviar orden de compra al nodo que lo tiene
            ip_destino, puerto_destino = nodo_con_articulo
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(5)
                    s.connect((ip_destino, puerto_destino))
                    s.sendall(json.dumps({
                        "tipo": "compra",
                        "origen": mensaje["origen"],  # para replicar el origen de la compra
                        "id_articulo": id_art,
                        "serie_articulo": serie_art,
                        "id_cliente": id_cli,
                        "fecha_envio": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "fecha": fecha
                    }).encode())
                    # Esperar confirmación
                    respuesta = s.recv(1024)
                    if respuesta != b"ok":
                        raise Exception("Respuesta inesperada del nodo")
            except Exception as e:
                print(f"[MAESTRO] Falló la compra con el nodo {ip_destino}: {e}")
                articulos_en_uso.remove((id_art, serie_art))
                return

            # Propagar a todos los nodos que la compra fue realizada
            enviar_a_todos({
                "tipo": "compra_realizada",
                "origen": [IP_LOCAL, PUERTO_NODO],
                "id_articulo": id_art,
                "serie_articulo": serie_art,
                "id_cliente": id_cli,
                "ubicacion": mensaje["ubicacion"],
                "fecha_envio": fecha
            })

            articulos_en_uso.remove((id_art, serie_art))

        elif tipo == "agregar_articulo":
            id_art = mensaje["id_articulo"]
            serie_art = mensaje["serie_articulo"]  # Nuevo: incluir número de serie
            nombre_art = mensaje["nombre_articulo"]
            cantidad = mensaje["cantidad"]

            with lock_inventario:
                inventario[(id_art, serie_art)]['cantidad'] = cantidad
                preexistente = coleccion_inventario.find_one({"id": int(id_art), "serie": int(serie_art)})
                max_id_art = coleccion_inventario.find_one({}, sort=[("id", -1)]) # aquí buscamos el id más alto
                # acutalizamos el id del artículo si es menor al más alto que tenemos
                if preexistente and preexistente["id"] < max_id_art["id"]:
                    id_art = max_id_art["id"] + 1
                    inventario[(id_art, serie_art)]["id"] = id_art
                    mensaje["id_articulo"] = id_art
                    print(f"[INFO] Actualizando ID del artículo {serie_art} a {id_art} (antes era {preexistente['id']})")
                if preexistente:
                    coleccion_inventario.find_one_and_replace(
                        {"id": int(id_art), "serie": int(serie_art)},
                        {"id": int(id_art), "nombre": nombre_art, "serie": int(serie_art), "cantidad": int(cantidad), "ubicacion": mensaje["ubicacion"]},
                        upsert=True
                    )
                else:
                    coleccion_inventario.insert_one({
                        "id": int(id_art),
                        "nombre": nombre_art,
                        "serie": int(serie_art),
                        "cantidad": int(cantidad),
                        "ubicacion": SUCURSAL
                    })
                log_local(f"Artículo {'cambiado' if preexistente else 'añadido'}: {id_art} (Serie: {serie_art}) (+{cantidad})")
                print(f"[INVENTARIO] {'Cambiado' if preexistente else 'Añadido'} {cantidad} unidades de {id_art} (Serie: {serie_art}) en {mensaje['ubicacion']}")
                # respondemos de hecho
                conn.sendall(b"ok")  # Al final del bloque de agregar artículo

        elif tipo == "cliente_update":

            if mensaje.get("origen") == [IP_LOCAL, PUERTO_NODO]:
                # Si no es de este nodo, no lo procesamos
                print("[INFO] Cliente update recibido de este nodo, ignorando")
                return

            id_cli = mensaje["id_cliente"]
            nombre = mensaje["nombre"]
            correo = mensaje.get("email", "")
            telefono = mensaje.get("telefono", "")
            
            preexistente = coleccion_clientes.find_one({"id": int(id_cli)})

            if preexistente:
                # Si el cliente ya existe, actualizamos sus datos
                log_local(f"Actualizando cliente: {id_cli} -> {nombre}")
                print(f"[CLIENTE] Actualizando: {id_cli} -> {nombre}")
                coleccion_clientes.find_one_and_replace(
                    {"id": int(id_cli)},
                    {"id": int(id_cli), "nombre": nombre, "email": correo, "telefono": telefono},
                    upsert=True
                )
                clientes[id_cli] = {
                    "nombre": nombre,
                    "email": correo,
                    "telefono": telefono
                }
                conn.sendall(b"ok")  # Al final del bloque de actualización de cliente
                return
            
            # Actualizamos el cliente en la base de datos
            coleccion_clientes.update_one({"id": int(id_cli)}, {
                "$set": {
                    "nombre": nombre,
                    "email": correo,
                    "telefono": telefono
                }
            }, upsert=True)
            # Actualizamos el registro local
            clientes[id_cli] = {
                "nombre": nombre,
                "email": correo,
                "telefono": telefono
            }
            log_local(f"Cliente actualizado: {id_cli} -> {nombre}")
            print(f"[CLIENTE] Actualizado: {id_cli} -> {nombre}")
            conn.sendall(b"ok")  # Al final del bloque de actualización de cliente

        elif tipo == "solicitar_estado":
            estado = {
                "inventario": list(coleccion_inventario.find({}, {"id": 0})),
                "clientes": list(coleccion_clientes.find({}, {"id": 0})),
                "guias": list(coleccion_guias.find({}, {"id": 0})),
                "sucursal": SUCURSAL,
                "operacion": int(OPERACION_ACTUAL),
                "timestamp": OPERACION_TIMESTAMP,  # Asegúrate de que tenga formato ISO
            }
            conn.sendall(json.dumps(estado).encode())

        elif tipo == "forzar_estado":
            OPERACION_ACTUAL = nuevo_estado.get("operacion", 0)
            OPERACION_TIMESTAMP = nuevo_estado.get("timestamp", "1970-01-01T00:00:00")

            nuevo_estado = mensaje["estado"]
            if not nuevo_estado or "inventario" not in nuevo_estado or "clientes" not in nuevo_estado:
                print("[ERROR] Estado forzado inválido recibido")
                conn.sendall(b"error")
                return
            coleccion_inventario.delete_many({})
            coleccion_inventario.insert_many(nuevo_estado["inventario"])
            coleccion_clientes.delete_many({})
            coleccion_clientes.insert_many(nuevo_estado["clientes"])
            log_local("Estado forzado con datos de consenso")
            print("[FORZADO] Estado sincronizado con consenso")

        elif tipo == "nuevo_maestro":
            MAESTRO_ACTUAL = mensaje["ip"]
            log_local(f"Cambio de maestro: nuevo maestro es {MAESTRO_ACTUAL}")
            print(f"[INFO] Nodo {MAESTRO_ACTUAL} es ahora el maestro")
        
        elif tipo == "articulo_agregado":
            id_art = mensaje["id_articulo"]
            origen = mensaje['origen']
            serie_art = mensaje["serie_articulo"]
            nombre_art = mensaje["nombre_articulo"]
            cantidad = mensaje["cantidad"]
            ubicacion = mensaje["ubicacion"]

            with lock_inventario:
                preexistente = coleccion_inventario.find_one({"id": int(id_art), "serie": int(serie_art)})
                coleccion_inventario.find_one_and_replace(
                    {"id": int(id_art), "serie": int(serie_art)},
                    {"id": int(id_art), "nombre": nombre_art, "serie": int(serie_art), "cantidad": int(cantidad), "ubicacion": ubicacion},
                    upsert=True
                )
                inventario[(id_art, serie_art)] = {
                    "id": id_art,
                    "nombre": nombre_art,
                    "serie": serie_art,
                    "cantidad": cantidad,
                    "ubicacion": ubicacion
                }
                log_local(f"Artículo {'actualizado' if preexistente else 'agregado'}: {id_art} (Serie: {serie_art}) (+{cantidad})")
                print(f"[INVENTARIO] {'Actualizado' if preexistente else 'Agregado'} {cantidad} unidades de {id_art} (Serie: {serie_art}) en {ubicacion}")
        elif tipo == "cliente_actualizado":
            id_cli = mensaje["id_cliente"]
            nombre = mensaje["nombre"]
            correo = mensaje.get("email", "")
            telefono = mensaje.get("telefono", "")
            
            clientes[id_cli] = {
                "nombre": nombre,
                "email": correo,
                "telefono": telefono
            }
            coleccion_clientes.update_one({"id": int(id_cli)}, {
                "$set": {
                    "nombre": nombre,
                    "email": correo,
                    "telefono": telefono
                }
            }, upsert=True)
            log_local(f"Cliente actualizado: {id_cli} -> {nombre}")
            print(f"[CLIENTE] Actualizado: {id_cli} -> {nombre}")
        
        elif tipo == "compra_realizada":
            id_art = mensaje["id_articulo"]
            serie_art = mensaje["serie_articulo"]
            id_cli = mensaje["id_cliente"]
            fecha_envio = mensaje["fecha_envio"]
            origen = mensaje["origen"]

            if origen == [IP_LOCAL, PUERTO_NODO]:
                # Este nodo ya hizo la compra
                return

            log_local(f"Compra replicada: {id_art} (Serie: {serie_art}) por cliente {id_cli}")
            print(f"[REPLICACIÓN] Confirmando compra de artículo {id_art} (Serie: {serie_art}) por cliente {id_cli}")

            with lock_inventario:
                if (id_art, serie_art) in inventario:
                    # NO reducir cantidad, solo asegurar sincronía
                    cantidad = inventario[(id_art, serie_art)]["cantidad"]
                    coleccion_inventario.find_one_and_replace(
                        {"id": int(id_art), "serie": int(serie_art)},
                        {
                            "id": int(id_art),
                            "nombre": inventario[(id_art, serie_art)]["nombre"],
                            "serie": int(serie_art),
                            "cantidad": cantidad,
                            "ubicacion": inventario[(id_art, serie_art)]["ubicacion"]
                        },
                        upsert=True
                    )
                else:
                    print(f"[REPLICACIÓN] Artículo {id_art} (Serie: {serie_art}) no encontrado")

            # Registrar la guía (esto sí puedes replicarlo)
            guia_codigo = f"{id_art}-{serie_art}-{inventario[(id_art, serie_art)]['ubicacion']}-{id_cli}"
            coleccion_guias.insert_one({
                "id": len(guias),
                "codigo": guia_codigo,
                "fecha_envio": fecha_envio,
                "estado": "Enviado"
            })
            guias[str(len(guias))] = {
                "codigo": guia_codigo,
                "fecha_envio": fecha_envio,
                "estado": "Enviado"
            }


def notificar_nuevo_maestro():
    msg = {"tipo": "nuevo_maestro", "ip": IP_LOCAL, "origen": [IP_LOCAL, PUERTO_NODO], "fecha_envio": time.strftime("%Y-%m-%d %H:%M:%S")}
    enviar_a_todos(msg)

# =====================
# Cliente para enviar mensajes
# =====================
def enviar_a_todos(mensaje):
    for ip, info in NODOS_DESCUBIERTOS.items():
        port = info["puerto"]
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((ip, port))
                s.sendall(json.dumps(mensaje).encode())
        except:
            print(f"[ERROR] No se pudo enviar a {ip}:{port}")

def enviar_a_maestro(mensaje):
    global MAESTRO_ACTUAL
    puerto_master = NODOS_DESCUBIERTOS.get(MAESTRO_ACTUAL, {}).get("puerto", PUERTO_NODO)
    if not SOY_MAESTRO and MAESTRO_ACTUAL:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((MAESTRO_ACTUAL, puerto_master))
                s.sendall(json.dumps(mensaje).encode())
        except:
            print(f"[ERROR] No se pudo enviar al maestro {MAESTRO_ACTUAL} sucursal {NODOS_DESCUBIERTOS[MAESTRO_ACTUAL]['sucursal']}")
    else:
        print("[INFO] Soy el maestro, no se envía al maestro")
        if( mensaje["tipo"] == "solicitar_compra"):
            # Si soy el maestro, no envío al maestro, sino que manejo la compra directamente
            # verificamos si el artículo está en inventario y en la sucursal correcta
            if(inventario[(mensaje["id_articulo"], mensaje["serie_articulo"])]["ubicacion"] != SUCURSAL):
                print(f"[ERROR] Artículo {mensaje['id_articulo']} (Serie: {mensaje['serie_articulo']}) no disponible en la sucursal actual.")
                enviar_a_todos(mensaje)
            else: # ejecutamos la compra directamente
                with lock_inventario: 
                    inventario[(mensaje["id_articulo"], mensaje["serie_articulo"])]["cantidad"] -= 1
                    coleccion_inventario.find_one_and_update(
                        {"id": int(mensaje["id_articulo"]), "serie": int(mensaje["serie_articulo"])},
                        {"$set": {
                            "cantidad": int(inventario[(mensaje["id_articulo"], mensaje["serie_articulo"])]["cantidad"]),
                            "ubicacion": SUCURSAL,
                            "nombre": inventario[(mensaje["id_articulo"], mensaje["serie_articulo"])]["nombre"]},
                        },
                        upsert=True
                    )
                    log_local(f"Compra realizada: {mensaje['id_articulo']} (Serie: {mensaje['serie_articulo']}) por cliente {mensaje['id_cliente']}")
                    guias[str(len(guias))] = {
                        "codigo": f"{mensaje['id_articulo']}-{mensaje['serie_articulo']}-{inventario[(mensaje['id_articulo'], mensaje['serie_articulo'])]['ubicacion']}-{mensaje['id_cliente']}",
                        "fecha_envio": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "estado": "Enviado",
                    }
                    # actualizamos la guía en la base de datos
                    coleccion_guias.insert_one({
                        "id": len(guias) - 1,
                        "codigo": guias[str(len(guias)-1)]["codigo"],
                        "fecha_envio": guias[str(len(guias)-1)]["fecha_envio"],
                        "estado": guias[str(len(guias)-1)]["estado"]
                    })

                    # actualizamos a todos los nodos
                    enviar_a_todos({
                        "tipo": "compra_realizada",
                        "origen": mensaje["origen"], # para replicar el origen de la compra
                        "id_articulo": mensaje["id_articulo"],
                        "serie_articulo": mensaje["serie_articulo"],
                        "id_cliente": mensaje["id_cliente"],
                        "fecha_envio": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "ubicacion": mensaje["ubicacion"]
                    })

        elif( mensaje["tipo"] == "agregar_articulo"):
            with lock_inventario:
                inventario[(mensaje["id_articulo"], mensaje["serie_articulo"])] = {
                    "id": mensaje["id_articulo"],
                    "nombre": mensaje["nombre_articulo"],
                    "serie": mensaje["serie_articulo"],
                    "cantidad": mensaje["cantidad"],
                    "ubicacion": mensaje["ubicacion"]
                }
                coleccion_inventario.find_one_and_update(
                    {"id": int(mensaje["id_articulo"]), "serie": int(mensaje["serie_articulo"])},
                    {"$set": {
                        "cantidad": int(mensaje["cantidad"]),
                        "ubicacion": mensaje["ubicacion"],
                        "nombre": mensaje["nombre_articulo"]},
                    },
                    upsert=True
                )
                log_local(f"Artículo agregado: {mensaje['id_articulo']} (Serie: {mensaje['serie_articulo']}) (+{mensaje['cantidad']})")
                enviar_a_todos({
                    "tipo": "articulo_agregado",
                    "origen": mensaje["origen"],  # para replicar el origen de la adición
                    "id_articulo": mensaje["id_articulo"],
                    "serie_articulo": mensaje["serie_articulo"],
                    "nombre_articulo": mensaje["nombre_articulo"],
                    "cantidad": mensaje["cantidad"],
                    "fecha_envio": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "ubicacion": mensaje["ubicacion"]
                })
        elif( mensaje["tipo"] == "cliente_update"):
            id_cli = mensaje["id_cliente"]
            nombre = mensaje["nombre"]
            correo = mensaje.get("email", "")
            telefono = mensaje.get("telefono", "")
            
            clientes[id_cli] = {
                "nombre": nombre,
                "email": correo,
                "telefono": telefono
            }
            coleccion_clientes.update_one({"id": int(id_cli)}, {
                "$set": {
                    "nombre": nombre,
                    "email": correo,
                    "telefono": telefono
                }
            }, upsert=True)
            log_local(f"Cliente actualizado: {id_cli} -> {nombre}")
            enviar_a_todos({
                "tipo": "cliente_actualizado",
                "origen": mensaje["origen"],  # para replicar el origen de la actualización
                "id_cliente": id_cli,
                "nombre": nombre,
                "email": correo,
                "fecha_envio": time.strftime("%Y-%m-%d %H:%M:%S"),
                "telefono": telefono
            })

# =====================
# Comparación de consenso (generales bizantinos)
# =====================
def comparar_datos():
    time.sleep(20)  # Esperar a que se estabilicen los nodos
    if not SOY_MAESTRO:
        return

    print("[CONSENSO] Comparando datos entre nodos")
    registros = {}

    for ip, info in NODOS_DESCUBIERTOS.items():
        try:
            puerto = info["puerto"]
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)  # Evita que se quede colgado si el nodo no responde
                s.connect((ip, puerto))
                s.sendall(json.dumps({"tipo": "solicitar_estado"}).encode())

                data = s.recv(8192)
                if not data:
                    print(f"[CONSENSO] Nodo {ip} respondió vacío")
                    continue

                raw_json = data.decode(errors="replace").strip()
                print(f"[CONSENSO] Respuesta cruda de {ip}:{puerto}:\n{raw_json}")

                try:
                    estado = json.loads(raw_json)
                except json.JSONDecodeError as je:
                    print(f"[CONSENSO] Error al decodificar JSON de {ip}:{puerto} -> {je}")
                    continue

                campos_requeridos = ["inventario", "clientes", "guias", "operacion", "timestamp"]
                if all(campo in estado for campo in campos_requeridos):
                    registros[ip] = estado
                else:
                    print(f"[CONSENSO] Nodo {ip} tiene campos faltantes")
        except Exception as e:
            print(f"[CONSENSO] Error con nodo {ip}:{info} -> {e}")
            continue

    if not registros:
        print("[CONSENSO] No se encontraron estados válidos para consenso")
        return

    def convertir_a_epoch(timestamp_str):
        try:
            # Formato ISO 8601 sin zona horaria: '2025-05-28T15:34:12'
            t = time.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S")
            return time.mktime(t)
        except:
            return 0  # Si falla, asumimos un timestamp muy antiguo

    # Determinar el mejor estado por operacion y timestamp en epoch
    def clave_prioridad(estado):
        op = int(estado.get("operacion", 0))
        ts = convertir_a_epoch(estado.get("timestamp", "1970-01-01T00:00:00"))
        return (op, ts)

    mejor_estado = max(registros.values(), key=clave_prioridad)
    print("[CONSENSO] Nodo con estado más confiable seleccionado")
    mejor_ip = max(registros.items(), key=lambda item: clave_prioridad(item[1]))[0]
    print(f"[CONSENSO] Nodo con mejor estado: {mejor_ip}")

    # Aplicar el estado consensuado
    coleccion_inventario.delete_many({})
    coleccion_inventario.insert_many(mejor_estado.get("inventario", []))

    coleccion_clientes.delete_many({})
    coleccion_clientes.insert_many(mejor_estado.get("clientes", []))

    coleccion_guias.delete_many({})
    coleccion_guias.insert_many(mejor_estado.get("guias", []))

    print("[CONSENSO] Estado actualizado según nodo más confiable")

    enviar_a_todos({
        "tipo": "forzar_estado",
        "origen": [IP_LOCAL, PUERTO_NODO],
        "estado": mejor_estado,
        "fecha_envio": time.strftime("%Y-%m-%d %H:%M:%S")
    })

# =====================
# Interfaz de comandos
# =====================
def interfaz():
    global SUCURSAL
    global SOY_MAESTRO, MAESTRO_ACTUAL
    global inventario, clientes, guias
    global NODOS_DESCUBIERTOS
    global lock_inventario

    while True:
        print("\n[Comandos] compra | cliente | agregar | ver | consenso | salir")
        cmd = input("> ").strip().lower()

        if cmd == "compra":
            id_art = input("ID Articulo: ")
            serie_art = input("Serie Artículo: ")
            id_cli = input("ID Cliente: ")
            msg = {
                "tipo": "solicitar_compra",
                "origen": (IP_LOCAL, PUERTO_NODO),
                "id_articulo": id_art,
                "serie_articulo": serie_art,
                "id_cliente": id_cli,
                "fecha_envio": time.strftime("%Y-%m-%d %H:%M:%S")
            }

            if( not id_cli in clientes):
                print(f"[ERROR] Cliente {id_cli} no encontrado.")
                continue

            if( not SOY_MAESTRO):
                enviar_a_maestro(msg)
            else:
                # comprobamos si está disponible
                if( serie_art not in inventario or inventario.get((id_art, serie_art), 0) <= 0):

                    # disponible en cantidad
                    if(inventario[(id_art, serie_art)]['cantidad'] <= 0):
                        print(f"[ERROR] Artículo {id_art} (Serie: {serie_art}) no disponible.")
                        continue

                    # disponible en otra sucursal
                    if(inventario.get((id_art, serie_art))["ubicacion"] != SUCURSAL):
                        print(f"[ERROR] Artículo {id_art} (Serie: {serie_art}) no disponible en la sucursal actual.")
                        enviar_a_todos(msg)
                    
                    # disponible en esta sucursal
                    else:
                        with lock_inventario:
                            inventario[(id_art, serie_art)]["cantidad"] -= 1
                            coleccion_inventario.find_one_and_update(
                            {"id": int(msg["id_articulo"]), "serie": int(serie_art)},
                            {"$set": {
                                "cantidad": int(inventario[(msg["id_articulo"], serie_art)]['cantidad']),
                                # "ubicacion": SUCURSAL,
                                "nombre": inventario[(msg["id_articulo"], serie_art)]['nombre']},
                            },
                            upsert=True
                        )
                        log_local(f"Compra realizada: {id_art} (Serie: {serie_art}) por cliente {id_cli}")
                        guias[str(len(guias))] = {
                            "codigo": f"{id_art}-{serie_art}-{inventario[(id_art, serie_art)]['ubicacion']}-{id_cli}",
                            "fecha_envio": time.strftime("%Y-%m-%d %H:%M:%S"),
                            "estado": "Enviado",
                        }
                        # actualizamos la guía en la base de datos
                        coleccion_guias.insert_one({
                            "id": len(guias) - 1,
                            "codigo": guias[str(len(guias)-1)]["codigo"],
                            "fecha_envio": guias[str(len(guias)-1)]["fecha_envio"],
                            "estado": guias[str(len(guias)-1)]["estado"]
                        })
                        # actualizamos a todos los nodos
                        enviar_a_todos({
                            "tipo": "compra_realizada",
                            "origen": (IP_LOCAL, PUERTO_NODO),  # para replicar el origen de la compra
                            "id_articulo": id_art,
                            "serie_articulo": serie_art,
                            "id_cliente": id_cli,
                            "fecha_envio": time.strftime("%Y-%m-%d %H:%M:%S"),
                            "ubicacion": SUCURSAL
                        })
                

        elif cmd == "agregar":
            print("Qué desea agregar?")
            print("1. Artículo")
            print("2. Cliente")
            subcmd = input("> ").strip().lower()
            if subcmd == "articulo" or subcmd == "artículo" or subcmd == "1":
                nombre_art = input("Nombre Artículo: ")
                serie_art = input("Serie Artículo: ")
                cantidad = int(input("Cantidad: "))
                msg = {
                    "tipo": "agregar_articulo", 
                    "origen": [IP_LOCAL, PUERTO_NODO],
                    "nombre_articulo": nombre_art,
                    "serie_articulo": serie_art, 
                    "id_articulo": str(len(inventario)),  # Generar ID único
                    "cantidad": cantidad, 
                    "ubicacion": SUCURSAL,
                    "fecha_envio": time.strftime("%Y-%m-%d %H:%M:%S")
                }
                enviar_a_maestro(msg)
                
            elif subcmd == "cliente" or subcmd == "2":
                nombre = input("Nombre Cliente: ")
                correo = input("Email Cliente: ")
                telefono = input("Teléfono Cliente: ")
                
                msg = {
                    "tipo": "cliente_update",
                    "id_cliente": str(len(clientes)),  # Generar ID único	
                    "origen": [IP_LOCAL, PUERTO_NODO],
                    "fecha_envio": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "nombre": nombre,
                    "email": correo,
                    "telefono": telefono
                }
                enviar_a_maestro(msg)

            else:
                print("Comando no reconocido.")

        elif cmd == "ver":
            print("Especifique qué desea ver:")
            print("1. inventario")
            print("2. clientes")
            print("3. guias")
            print("4. sucursal actual")
            print("5. nodos")
            print("6. maestro")
            subcmd = input("> ").strip().lower()
            if subcmd == "inventario" or subcmd == "1":
                print("---------Inventario---------")
                for item in inventario.values():
                    print(f"ID: {item['id']} Nombre: {item['nombre']}, Serie: {item['serie']}, Cantidad: {item['cantidad']}, Ubicación: {item['ubicacion']}")
            elif subcmd == "clientes" or subcmd == "2":
                print("---------Clientes---------")
                for cliente in clientes.values():
                    print(f"ID: {cliente['id']} Nombre: {cliente['nombre']}, Email: {cliente['email']}, Teléfono: {cliente['telefono']}")
            elif subcmd == "guias" or subcmd == "3":
                print("---------Envios---------")
                for guia in guias.values():
                    print(f"Código: {guia['codigo']}, Fecha: {guia['fecha_envio']}, Estado: {guia['estado']}")
            elif subcmd == "sucursal actual" or subcmd == "4":
                print(f"Sucursal actual: {SUCURSAL}")
            elif subcmd == "nodos" or subcmd == "5":
                print("---------Nodos Descubiertos---------")
                for ip, port in NODOS_DESCUBIERTOS.items():
                    print(f"IP: {ip}, Puerto: {port}")
            elif subcmd == "maestro" or subcmd == "6":
                if MAESTRO_ACTUAL:
                    print(f"Maestro actual: {MAESTRO_ACTUAL}")
                else:
                    if( not SOY_MAESTRO):
                        print("Esperando elección de maestro...")
                    else:
                        print("Soy el maestro actual.")
            else:
                print("Comando no reconocido.")

        elif cmd == "consenso":
            comparar_datos()

        elif cmd == "salir":
            break

        # comando oculto
        elif cmd == "debug":
            print("Nodos descubiertos:", NODOS_DESCUBIERTOS)
            print("Maestro actual:", MAESTRO_ACTUAL)
            print("Inventario:", inventario)
            print("Clientes:", clientes)
            print("Guias:", guias)
            
        elif cmd == "set":
            SUCURSAL = input("Nueva sucursal: ")

        elif cmd == "borrar": # borra las bases de datos
            coleccion_inventario.delete_many({})
            coleccion_clientes.delete_many({})
            coleccion_guias.delete_many({})
            inventario.clear()
            clientes.clear()
            guias.clear()
            print("[INFO] Bases de datos borradas.")
            break

# =====================
# Main
# =====================
threading.Thread(target=enviar_broadcast, daemon=True).start()
threading.Thread(target=escuchar_broadcast, daemon=True).start()
threading.Thread(target=servidor_nodo, daemon=True).start()
threading.Thread(target=elegir_maestro, daemon=True).start()
threading.Thread(target=verificar_maestro, daemon=True).start()

if __name__ == "__main__":
    if(IP_LOCAL == max(lista_sucursales)):
        SOY_MAESTRO = True
        MAESTRO_ACTUAL = IP_LOCAL
        print("[INFO] Soy el nodo maestro inicial")
        threading.Thread(target=comparar_datos, daemon=True).start()
    interfaz()
