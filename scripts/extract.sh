export INPUT_DATABASE_URL="postgresql://pguser:password@localhost:5432/mydb"
export SEMANTIC_MODEL_URL="http://localhost:5000/upload-and-predict"
export DB_DIALECT="postgres"
export OUTPUT_FILE="/tmp/output_test3.json"

SCRIPT_PATH="$(dirname -- "${BASH_SOURCE[0]}")"
SCRIPT_PATH="$(cd -- "$SCRIPT_PATH" && pwd)"

source "$SCRIPT_PATH/../.venv/bin/activate"
python "$SCRIPT_PATH/../noni/extractor/main.py"