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
IP_LOCAL = socket.gethostbyname(socket.gethostname())
MAESTRO_ACTUAL = None

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
                            ips = list(NODOS_DESCUBIERTOS.keys()) + [IP_LOCAL]
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
                    s.connect((MAESTRO_ACTUAL, PUERTO_NODO))
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
    with conn:
        data = conn.recv(8192)
        if not data:
            return
        mensaje = json.loads(data.decode())
        tipo = mensaje.get("tipo")

        if tipo == "ping":
            conn.sendall(b"pong")

        elif tipo == "compra":
            serie_art = mensaje["serie_articulo"]  # Nuevo: incluir número de serie
            id_cli = mensaje["id_cliente"]
            fecha = mensaje["fecha_envio"]
            id_art = mensaje["id_articulo"]

            with lock_inventario:
                if( serie_art not in inventario or inventario.get((id_art, serie_art), 0) <= 0):
                    nombre_objeto = coleccion_inventario.find_one({"serie": int(serie_art)}, {"nombre": 1, "id": 0})
                    print(f"[ERROR] Artículo {nombre_objeto}(Serie: {serie_art}) no disponible.")
                    return
                
                inventario.find_one({ "serie": serie_art }, { "cantidad": 1, "id": 0 })
                inventario[(id_art, serie_art)]['cantidad'] -= 1
                if inventario[(id_art, serie_art)] < 0:
                    print(f"[ERROR] No hay suficiente inventario para el artículo {serie_art}.")
                    return
                coleccion_inventario.find_one_and_update(
                    {"id": int(id_art), "serie": int(serie_art)},
                    {"$set": {
                        "cantidad": int(inventario[(id_art, serie_art)]['cantidad']),
                        "ubicacion": SUCURSAL,
                        "nombre": inventario[(id_art, serie_art)]['nombre']},
                    },
                    upsert=True
                )
                log_local(f"Compra realizada: (Serie: {serie_art}) por cliente {id_cli} en {fecha}")
                print(f"[COMPRA] Artículo (Serie: {serie_art}) vendido a cliente {id_cli} en {fecha}")
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
                        for art in estado.get("inventario", []):
                            if str(art["id"]) == id_art and str(art["serie"]) == serie_art and art["cantidad"] > 0:
                                nodo_con_articulo = (ip, info["puerto"])
                                break
                except Exception as e:
                    print(f"[WARN] No se pudo consultar el nodo {ip}: {e}")
                if nodo_con_articulo:
                    break

            if not nodo_con_articulo:
                print(f"[MAESTRO] Ningún nodo tiene el artículo {id_art} (Serie: {serie_art}). Me lo adjudico.")
                with lock_inventario:
                    inventario[(id_art, serie_art)] = {
                        "id": id_art,
                        "serie": serie_art,
                        "nombre": f"Desconocido {id_art}",
                        "cantidad": 1,
                        "ubicacion": SUCURSAL
                    }
                    coleccion_inventario.insert_one({
                        "id": int(id_art),
                        "serie": int(serie_art),
                        "nombre": f"Desconocido {id_art}",
                        "cantidad": 1,
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
                        "id_articulo": id_art,
                        "serie_articulo": serie_art,
                        "id_cliente": id_cli,
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
                "origen": (IP_LOCAL, PUERTO_NODO),
                "id_articulo": id_art,
                "serie_articulo": serie_art,
                "id_cliente": id_cli,
                "ubicacion": SUCURSAL,
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
                try:
                    coleccion_inventario.find_one_and_replace(
                        {"id": int(id_art), "serie": int(serie_art)},
                        {"id": int(id_art), "nombre": nombre_art, "serie": int(serie_art), "cantidad": int(cantidad), "ubicacion": mensaje["ubicacion"]},
                        upsert=True
                    )
                except Exception as e:
                    coleccion_inventario.insert_one({
                        "id": int(id_art),
                        "nombre": nombre_art,
                        "serie": int(serie_art),
                        "cantidad": int(cantidad),
                        "ubicacion": SUCURSAL
                    })
                log_local(f"Artículo agregado: {id_art} (Serie: {serie_art}) (+{cantidad})")
                print(f"[INVENTARIO] Agregado {cantidad} unidades de {id_art} (Serie: {serie_art})")
        
        elif tipo == "cliente_update":
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

        elif tipo == "solicitar_estado":
            estado = {
                "inventario": list(coleccion_inventario.find({}, {"id": 0})),
                "clientes": list(coleccion_clientes.find({}, {"id": 0}))
            }
            conn.sendall(json.dumps(estado).encode())

        elif tipo == "forzar_estado":
            nuevo_estado = mensaje["estado"]
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
            serie_art = mensaje["serie_articulo"]
            nombre_art = mensaje["nombre_articulo"]
            cantidad = mensaje["cantidad"]
            ubicacion = mensaje["ubicacion"]

            with lock_inventario:
                inventario[(id_art, serie_art)] = {
                    "id": id_art,
                    "nombre": nombre_art,
                    "serie": serie_art,
                    "cantidad": cantidad,
                    "ubicacion": ubicacion
                }
                coleccion_inventario.find_one_and_update(
                    {"id": int(id_art), "serie": int(serie_art)},
                    {"$set": {
                        "cantidad": int(cantidad),
                        "ubicacion": ubicacion,
                        "nombre": nombre_art},
                    },
                    upsert=True
                )
                log_local(f"Artículo agregado: {id_art} (Serie: {serie_art}) (+{cantidad})")
                print(f"[INVENTARIO] Agregado {cantidad} unidades de {id_art} (Serie: {serie_art}) en {ubicacion}")
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
            ubicacion = mensaje["ubicacion"]

            log_local(f"Compra realizada: {id_art} (Serie: {serie_art}) por cliente {id_cli} en {ubicacion}")
            print(f"[COMPRA] Artículo {id_art} (Serie: {serie_art}) vendido a cliente {id_cli} en {ubicacion}")
            # actualizamos nuestro registro de inventario
            with lock_inventario:
                if (id_art, serie_art) in inventario:
                    inventario[(id_art, serie_art)]["cantidad"] -= 1
                    if inventario[(id_art, serie_art)]["cantidad"] < 0:
                        print(f"[ERROR] No hay suficiente inventario para el artículo {id_art} (Serie: {serie_art}).")
                        return
                    coleccion_inventario.find_one_and_update(
                        {"id": int(id_art), "serie": int(serie_art)},
                        {"$set": {
                            "cantidad": int(inventario[(id_art, serie_art)]["cantidad"]),
                            "ubicacion": ubicacion,
                            "nombre": inventario[(id_art, serie_art)]["nombre"]},
                        },
                        upsert=True
                    )
                else:
                    print(f"[ERROR] Artículo {id_art} (Serie: {serie_art}) no encontrado en el inventario local.")
                    # pedimos consenso para actualizar el inventario
                    enviar_a_todos({
                        "tipo": "solicitar_estado",
                        "origen": (IP_LOCAL, PUERTO_NODO),
                    })
            # registrar guía de envío
            guia_codigo = f"{id_art}-{serie_art}-{ubicacion}-{id_cli}"
            coleccion_guias.insert_one({
                "id": len(guias),
                "codigo": guia_codigo,
                "fecha_envio": mensaje["fecha_envio"],
                "estado": "Enviado",
            })

def notificar_nuevo_maestro():
    msg = {"tipo": "nuevo_maestro", "ip": IP_LOCAL}
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
    if not SOY_MAESTRO and MAESTRO_ACTUAL:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((MAESTRO_ACTUAL, PUERTO_NODO))
                s.sendall(json.dumps(mensaje).encode())
        except:
            print(f"[ERROR] No se pudo enviar al maestro {MAESTRO_ACTUAL}")
    else:
        print("[INFO] Soy el maestro, no se envía al maestro")
        if( mensaje["tipo"] == "solicitar_compra"):
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
                        "ubicacion": SUCURSAL
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
    for ip, port in NODOS_DESCUBIERTOS.items():
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((ip, port))
                s.sendall(json.dumps({"tipo": "solicitar_estado"}).encode())
                data = s.recv(8192)
                registros[ip] = json.loads(data.decode())
        except:
            continue
    votos = {}
    for ip, estado in registros.items():
        inventario = tuple(sorted((k, v) for k, v in estado["inventario"].items()))
        clientes = tuple(sorted((k, v) for k, v in estado["clientes"].items()))
        votos[(inventario, clientes)] = votos.get((inventario, clientes), 0) + 1
    # Encontrar el estado más votado
    try:
        estado_mas_votado = max(votos, key=votos.get)
        inventario, clientes = estado_mas_votado
        print("[CONSENSO] Estado más votado encontrado")
        # Forzar el estado en la base de datos
        coleccion_inventario.delete_many({})
        coleccion_inventario.insert_many([{"id": k[0], "serie": k[1], "nombre": v["nombre"], "cantidad": v["cantidad"], "ubicacion": v["ubicacion"]} for k, v in inventario])
        coleccion_clientes.delete_many({})
        coleccion_clientes.insert_many([{"id": k[0], "nombre": v["nombre"], "email": v["email"], "telefono": v["telefono"]} for k, v in clientes])
        # Notificar a todos los nodos
        msg = {
            "tipo": "forzar_estado",
            "estado": {
                "inventario": [{"id": k[0], "serie": k[1], "nombre": v["nombre"], "cantidad": v["cantidad"], "ubicacion": v["ubicacion"]} for k, v in inventario],
                "clientes": [{"id": k[0], "nombre": v["nombre"], "email": v["email"], "telefono": v["telefono"]} for k, v in clientes]
            }
        }
    except ValueError:
        print("[CONSENSO] No se encontraron estados válidos para consenso")
        return

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
                "origen": (IP_LOCAL,PUERTO_NODO),
                "id_articulo": id_art,
                "serie_articulo": serie_art,
                "id_cliente": id_cli,
                "fecha_envio": time.strftime("%Y-%m-%d %H:%M:%S")
            }

            if( not SOY_MAESTRO):
                enviar_a_maestro(msg)
            else:
                # comprobamos si está en nuestra sucursal
                if( serie_art not in inventario or inventario.get((id_art, serie_art), 0) <= 0):
                    if(inventario[(id_art, serie_art)]['cantidad'] <= 0):
                        print(f"[ERROR] Artículo {id_art} (Serie: {serie_art}) no disponible.")
                        continue
                    if(inventario.get((id_art, serie_art))["ubicacion"] != SUCURSAL):
                        print(f"[ERROR] Artículo {id_art} (Serie: {serie_art}) no disponible en la sucursal actual.")
                        enviar_a_todos(msg)
                    else:
                        with lock_inventario:
                            inventario[(id_art, serie_art)]["cantidad"] -= 1
                            coleccion_inventario.find_one_and_update(
                            {"id": int(msg["id_articulo"]), "serie": int(serie_art)},
                            {"$set": {
                                "cantidad": int(inventario[(msg["id_articulo"], serie_art)]['cantidad']),
                                "ubicacion": SUCURSAL,
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

        elif cmd == "agregar":
            print("Qué desea agregar?")
            print("1. Artículo")
            print("2. Cliente")
            subcmd = input("> ").strip().lower()
            if subcmd == "artículo":
                nombre_art = input("Nombre Artículo: ")
                serie_art = input("Serie Artículo: ")
                cantidad = int(input("Cantidad: "))
                msg = {
                    "tipo": "agregar_articulo", 
                    "nombre_articulo": nombre_art,
                    "serie_articulo": serie_art, 
                    "cantidad": cantidad, 
                    "ubicacion": SUCURSAL
                }
                enviar_a_maestro(msg)
                
            elif subcmd == "cliente":
                nombre = input("Nombre Cliente: ")
                correo = input("Email Cliente: ")
                telefono = input("Teléfono Cliente: ")
                
                msg = {
                    "tipo": "cliente_update",
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
            if subcmd == "inventario":
                print("---------Inventario---------")
                for item in inventario.values():
                    print(f"ID: {item['id']} Nombre: {item['nombre']}, Serie: {item['serie']}, Cantidad: {item['cantidad']}, Ubicación: {item['ubicacion']}")
            elif subcmd == "clientes":
                print("---------Clientes---------")
                for cliente in clientes.values():
                    print(f"ID: {cliente['id']} Nombre: {cliente['nombre']}, Email: {cliente['email']}, Teléfono: {cliente['telefono']}")
            elif subcmd == "guias":
                print("---------Envios---------")
                for guia in guias.values():
                    print(f"Código: {guia['codigo']}, Fecha: {guia['fecha_envio']}, Estado: {guia['estado']}")
            elif subcmd == "sucursal actual":
                print(f"Sucursal actual: {SUCURSAL}")
            elif subcmd == "nodos":
                print("---------Nodos Descubiertos---------")
                for ip, port in NODOS_DESCUBIERTOS.items():
                    print(f"IP: {ip}, Puerto: {port}")
            elif subcmd == "maestro":
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
