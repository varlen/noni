# TODO

[X] Fix serde issues with polymorphism on ColumnMetadata class
[X] Check why some columns doesn't have any samples even with sample counts


[ ] Assemble minitables for Sato using samples
[ ] Integrate sato using HTTP for textual data
    [ ] Run Sato server
        [X] Port Sato scripts for Windows
        [X] Download NLTK data for table topic feature extraction
        [X] Fix gensim.Doc2Vec model loading
            https://programmerah.com/python-gensim-attributeerror-doc2vec-object-has-no-attribute-dv-36258/
            https://stackoverflow.com/questions/50237247/gensim-doc2vec-object-has-no-attribute-intersect-word2vec-format-when-i-loa

            pip uninstall gensim
            pip install gensim==3.8.3

        [X] /upload-and-predict endpoint de predição do csv. Arquivo deve estar como 'file'
            no request

        [X] Increase maximum number of columns 
        

[ ] Detectar distribuição de amostras numéricas (é gaussiano? uniforme?)

[ ] Configure using env vars (ex: number of samples)


# Limitações
- caso das colunas units_on_stock e units_on_order (regras implicitas entre os dados)
- Somente postgres
- Colunas de chave estrangeira ou autoreferenciadas (representação de grafos (dados categoricos porém representados como números inteiros))

# Validação
- Banco de dados Northwind