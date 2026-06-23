#!/usr/bin/env bash
# install.sh - Registra el MCP server clawbus en la configuración de Claude Code.
set -euo pipefail

# 1. Detectar archivo de configuración objetivo
CONFIG_FILE=""
if [ -f "$HOME/.claude.json" ] && grep -q '"mcpServers"' "$HOME/.claude.json"; then
  CONFIG_FILE="$HOME/.claude.json"
elif [ -f "$HOME/.claude/settings.json" ] && grep -q '"mcpServers"' "$HOME/.claude/settings.json"; then
  CONFIG_FILE="$HOME/.claude/settings.json"
elif [ -f "$HOME/.claude.json" ]; then
  CONFIG_FILE="$HOME/.claude.json"
elif [ -f "$HOME/.claude/settings.json" ]; then
  CONFIG_FILE="$HOME/.claude/settings.json"
else
  # Si ninguno existe, por defecto usar ~/.claude.json
  CONFIG_FILE="$HOME/.claude.json"
fi

echo "Archivo de configuración detectado: $CONFIG_FILE"

# 2. Hacer backup antes de tocar nada
if [ -f "$CONFIG_FILE" ]; then
  BACKUP_FILE="${CONFIG_FILE}.backup.$(date +%s)"
  cp "$CONFIG_FILE" "$BACKUP_FILE"
  echo "Backup creado en: $BACKUP_FILE"
else
  # Si el archivo no existe, creamos una estructura vacía
  echo '{}' > "$CONFIG_FILE"
fi

# 3. Registrar clawbus en mcpServers usando Node/Bun (o JQ si fallan)
JS_CODE='
const fs = require("fs");
const file = process.argv[2];
let data = {};
if (fs.existsSync(file)) {
  try {
    data = JSON.parse(fs.readFileSync(file, "utf8"));
  } catch (e) {
    console.error("Error al parsear el JSON de: " + file + ": " + e.message);
    process.exit(1);
  }
}
if (!data.mcpServers) {
  data.mcpServers = {};
}
data.mcpServers["clawbus"] = {
  type: "stdio",
  command: "bun",
  args: ["/workspace/clawbus/clawbus-channel/server.ts"],
  env: {}
};
fs.writeFileSync(file, JSON.stringify(data, null, 2), "utf8");
console.log("Servidor clawbus registrado correctamente en " + file);
'

if command -v node >/dev/null 2>&1; then
  node -e "$JS_CODE" "$CONFIG_FILE"
elif command -v bun >/dev/null 2>&1; then
  bun -e "$JS_CODE" "$CONFIG_FILE"
elif command -v jq >/dev/null 2>&1; then
  jq '.mcpServers.clawbus = {"type": "stdio", "command": "bun", "args": ["/workspace/clawbus/clawbus-channel/server.ts"], "env": {}}' "$CONFIG_FILE" > "$CONFIG_FILE.tmp" && mv "$CONFIG_FILE.tmp" "$CONFIG_FILE"
  echo "Servidor clawbus registrado correctamente en $CONFIG_FILE"
else
  echo "Error: No se pudo encontrar node, bun ni jq para modificar el archivo de configuración."
  echo "Por favor, añade manualmente el siguiente fragmento a 'mcpServers' en tu configuración:"
  cat << 'EOFMCP'
  "clawbus": {
    "type": "stdio",
    "command": "bun",
    "args": [
      "/workspace/clawbus/clawbus-channel/server.ts"
    ],
    "env": {}
  }
EOFMCP
  exit 1
fi

# 4. Imprimir instrucciones exactas
echo ""
echo "Instrucciones de ejecución:"
echo "Lanza Claude con el comando exacto:"
echo "--------------------------------------------------------------------------------"
echo "CLAWBUS_AGENT=<id> CLAWBUS_ROOM=<room> CLAWBUS_BROKER_URL=ws://localhost:18791 \\"
echo "  claude --channels plugin:telegram@claude-plugins-official,server:clawbus"
echo "--------------------------------------------------------------------------------"
