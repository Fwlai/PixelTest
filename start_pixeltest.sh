#!/data/data/com.termux/files/usr/bin/bash

PROJECT_DIR="$HOME/pixeltest"
SPRITE_SOURCE="/sdcard/Download/Zeno_Characters/sprite1.png"

mkdir -p "$PROJECT_DIR"
cd "$PROJECT_DIR" || exit 1

if [ ! -f "sprites.png" ]; then
    if [ -f "$SPRITE_SOURCE" ]; then
        cp "$SPRITE_SOURCE" "sprites.png"
    else
        echo "Error: sprite file not found."
        exit 1
    fi
fi

echo "Pixeltest"
echo "Server: http://localhost:8080"
echo "Stop: Ctrl + C"

python -m http.server 8080 --bind 127.0.0.1
