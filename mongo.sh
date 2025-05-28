#!/bin/bash

set -e
# Instalar fish
echo "🔧 Instalando fish..."
apt update && apt install -y fish

#Instalar gnupg
apt install gnupg curl

# Establecer fish como shell por defecto para el usuario actual
echo "🔧 Estableciendo fish como shell predeterminado..."
chsh -s /usr/bin/fish "$(whoami)"

# 1. Agregar repositorio MongoDB
echo "➤ Agregando repositorio de MongoDB..."
echo "deb [arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg] https://repo.mongodb.org/apt/debian bookworm/mongodb-org/7.0 main" | tee /etc/apt/sources.list.d/mongodb-org-7.0.list

# 2. Importar la clave GPG
echo "➤ Descargando y guardando la clave GPG..."
curl -fsSL https://pgp.mongodb.com/server-7.0.asc | gpg --dearmor -o /usr/share/keyrings/mongodb-server-7.0.gpg

# 3. Actualizar e instalar MongoDB
echo "➤ Actualizando paquetes e instalando MongoDB..."
apt update
apt install -y mongodb-org

# 4. Iniciar y habilitar MongoDB
echo "➤ Iniciando y habilitando el servicio de MongoDB..."
systemctl start mongod
systemctl enable mongod

# 5. Verificación de instalación
echo "➤ Verificando conexión con MongoDB..."
mongosh --eval 'db.runCommand({ connectionStatus: 1 })'

echo "✅ MongoDB instalado y ejecutándose correctamente."