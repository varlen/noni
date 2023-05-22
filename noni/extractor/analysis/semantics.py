DEFAULT_MODEL_URL = "http://localhost:5000/upload-and-predict"

def run_semantic_inference_model(table, csv_str, model_url = DEFAULT_MODEL_URL):
    table_name = table['name']
    column_names = [ c['name'] for c in table['columns']]
    file_name = table_name.lower() + '.csv'
    files = { 'file': (file_name, csv_str)}
    inferences = requests.post(model_url, files=files).json()
    return dict(zip(column_names, inferences))

def csv_escape(txt):
    return str(txt).replace(',','.').replace('\n','\t')

def generate_csv_from_table(engine, db, table, plugin, max_rows = 50):
    column_names = [ column['name'] for column in table['columns']]
    select_rows_query = get_select_rows_query(column_names, table['schema'], table['name'], max_rows)
    loaded_rows = db.get(engine, select_rows_query)
    if not len(loaded_rows):
        print(f"[red]  No csv sample for table '{table['name']}'[/red]")
        return None

    csv_rows = [','.join(column_names)]
    for row in loaded_rows:
        csv_rows.append(','.join([ csv_escape(value) for value in row ]))
    print(f"  CSV sample for table '{table['name']}' ready")
    return '\n'.join(csv_rows)

def add_semantic_metadata_to_table(engine, db, table, plugin):
    csv_table_sample = generate_csv_from_table(engine, db, table, plugin)
    if csv_table_sample:
        semantic_inferences = run_semantic_inference_model(table, csv_table_sample)
        for column in table['columns']:
            if column['name'] in semantic_inferences:
                inferred_category = semantic_inferences[column['name']]
                if not 'metadata' in column or not column['metadata']:
                    column['metadata'] = {}
                column['metadata']['semanticClass'] = inferred_category