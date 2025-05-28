import json
import subprocess
import os

# Archivo con los datos corregidos (puede ser mongo.json o mongo_corregido.json)
input_file = "mongo_corregido.json"

# Base de datos y colecciones
db_name = "sistema_distribuido"
collections = ["inventario", "clientes", "guias"]

def separar_colecciones():
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    for coleccion in collections:
        filename = f"{coleccion}.json"
        with open(filename, "w", encoding="utf-8") as f_out:
            json.dump(data[coleccion], f_out, indent=4, ensure_ascii=False)
        print(f"[INFO] Archivo {filename} creado con {len(data[coleccion])} documentos.")

def importar_coleccion(coleccion):
    filename = f"{coleccion}.json"
    if not os.path.isfile(filename):
        print(f"[ERROR] Archivo {filename} no encontrado. No se puede importar.")
        return
    comando = [
        "mongoimport",
        "--db", db_name,
        "--collection", coleccion,
        "--file", filename,
        "--jsonArray",
        "--drop"  # Elimina la colección antes de importar para evitar duplicados
    ]
    print(f"[INFO] Importando colección {coleccion} desde {filename}...")
    resultado = subprocess.run(comando, capture_output=True, text=True)
    if resultado.returncode == 0:
        print(f"[OK] Colección {coleccion} importada correctamente.")
    else:
        print(f"[ERROR] Falló la importación de {coleccion}:")
        print(resultado.stderr)

def main():
    separar_colecciones()
    for coleccion in collections:
        importar_coleccion(coleccion)

if __name__ == "__main__":
    main()
