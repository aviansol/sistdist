import json

# Abre el archivo original mongo.json
with open("mongo.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# Recorre cada colección para agregar el campo "id" si no existe
for coleccion in ["inventario", "clientes", "guias"]:
    for i, doc in enumerate(data[coleccion]):
        if "id" not in doc or doc["id"] is None:
            # Si no tiene "id", asigna el índice como id (puedes cambiarlo por otro criterio)
            doc["id"] = i

# Guarda el archivo corregido para importar luego
with open("mongo_corregido.json", "w", encoding="utf-8") as f:
    json.dump(data, f, indent=4, ensure_ascii=False)

print("Archivo mongo_corregido.json generado con campo 'id' agregado donde faltaba.")