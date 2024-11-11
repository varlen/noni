export OUTPUT_DATABASE_URL="postgresql://pguser:password@localhost:5432/outputnorthwind"

SCRIPT_PATH="$(dirname -- "${BASH_SOURCE[0]}")"
SCRIPT_PATH="$(cd -- "$SCRIPT_PATH" && pwd)"

NONI_SPEC_FILE="$SCRIPT_PATH/../data/northwind-extractor-output.json"

source "$SCRIPT_PATH/../venv2/bin/activate"
python "$SCRIPT_PATH/../noni/generator/main.py" $NONI_SPEC_FILE --structure --data