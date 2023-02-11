# TODO

[X] Fix serde issues with polymorphism on ColumnMetadata class
[X] Check why some columns doesn't have any samples even with sample counts


[X] Assemble minitables for Sato using samples
[X] Integrate sato using HTTP for textual data
    [X] Run Sato server
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


[ ] Implement generators (python)

    [x] Add a way to populate data from samples
    [x] Map sato types (type78) to Faker generators
    [ ] Implement numeric generators

    ----- ALL OF THESE ARE ORM RESPONSABILITIES, USE SQL ALCHEMY
    https://stackoverflow.com/questions/973481/dynamic-table-creation-and-orm-mapping-in-sqlalchemy
    [x] Create function to create tables dinamically using the ORM
    [x] Use spec to create tables using the function
    [x] Create data dinamically (data dicts from table spec)
        - create data dicts from table -> https://stackoverflow.com/questions/29455436/how-to-create-classes-from-existing-tables-using-flask-sqlaclhemy
        - create class dinamically -> https://python-course.eu/oop/dynamically-creating-classes-with-type.php
        - https://stackoverflow.com/questions/35372970/dynamic-table-names-with-sqlalchemy
        - https://docs.sqlalchemy.org/en/14/core/metadata.html#sqlalchemy.schema.Table

    [ ] Generate inserts using data generators

    [ ] Map tables and models
    [ ] Populate models with generators


[ ] Detectar distribuição de amostras numéricas (é gaussiano? uniforme?)

[ ] Detectar colunas UNIQUE pela moda
[ ] Detectar colunas NULLABLE e adicionar aos geradores

[ ] Extract foreign key relations

[ ] Configure using env vars (ex: number of samples)


# Limitações
- caso das colunas units_on_stock e units_on_order (regras implicitas entre os dados)
- Somente postgres
- Colunas não unitárias (Array, JSON, BJSON)
- Colunas de chave estrangeira ou autoreferenciadas (representação de grafos (dados categoricos porém representados como números inteiros))
- Algumas possíveis classificações geradas pelo dataset pré-treinado type78 do Sato, detectam dados potencialmente numéricos como elevation, weight, year e rank. No entanto, se a coluna que contém esses dados não for do tipo texto, a informação de classificação não será utilizada.
- Algumas classificações criam geradores que retornam amostras dos dados originais. Esse comportamento pode ser indesejado se o conteúdo de cada amostra for um dado sensível. Deve haver a possibilidade de suprimir essas amostras se for da necessidade do usuário.
# Validação
- Banco de dados Northwind

### 
2023

Reescrevi o extractor em python para facilitar as mudanças.

Falta:

- Usar o resultado da query de keys para enriquecer os metadados (extractor-py)
- Usar os metadados de key para ordenar a criação das tabelas/relações de foreign key
    - Quase, falta implementar o retry de tabelas
- Refactor do generator para considerar schema + table_name nas chamadas e dicts
- Possíveis improvements de metadados (ver acima)
- Portar para rodar em outros computadores (container SATO + volume com modelo)

https://stackoverflow.com/questions/11875770/how-to-overcome-datetime-datetime-not-json-serializable

Preciso garantir que as colunas estão ordenadas pelas dependências de foreign key

# Problema: Qual é a ordem de criação das tabelas dadas as dependências de foreign key?
    - Uma FK pode apontar para a própria tabela
    - Uma tabela pode ter mais de uma FK
    

    
## Modelando como grafo:
- O algoritmo de data generation recebe um Grafo de Tabelas e retorna uma lista ordenada de Datasets preenchidos de acordo com as dependencias entre tabelas
- Grafo de Tabelas ---> [ Algoritmo ] ---> Lista de Datasets
- Os datasets podem ser transformados em INSERT commands
    - Tabela : nó
    - Relação FK->PK : Vértice    (FK) -----depends-----> (PK)
    - Uma tabela que têm FK depende da sua PK existir para poder ser criada
    - Analisando pelo caso especial em que há uma FK apontando para PK na mesma tabela (autorreferência), existem duas alternativas:
        - Se a coluna FK é Nullable: Gerar a tabela com os valores dessa coluna preenchidos como NULL
        - Se a coluna FK não é Nullable, a primeira linha da tabela deve ser criada com FK = PK (ex: PK=1) e as linhas subsequentes com amostras dos valores anteriores da PK.
            - Depois, pode-se randomizar os valores das FK para amostras da PK.
    - Sob essa ótica, as relações entre PK e FK podem ser modeladas como um Directed Acyclic Graph (DAG) quando não houverem tabelas cuja FK apontam para uma PK na própria tabela. Então, se a FK apontar para a própria tabela, essa será desconsiderada pois já foi processada pelo caso anterior.
    - Um detalhe a se observar é que o grafo pode conter nós desconexos e o algoritmo deve levar essa possibilidade em consideração, já que existem tabelas que não são dependentes ou dependências.
    - Então, usamos um algoritmo de Topological Sorting:
        - Para cada tabela:
                - Usamos uma pilha temporária compartilhada
                - Chamamos o topological sorting para todos os vértices (tabelas que contém a PK apontada pelas FKs da tabela atual)
                - Adicionamos a tabela a pilha. (Uma tabela só entra na pilha quando as tabelas PK já estão na pilha)
            - Imprimimos o conteúdo da pilha
        - Implementação recursiva:
            - Criar uma pilha para armazenar os nós (a saída temporária)
            - Set de visited
            - Loop: P/ todas as tabelas:
                - Chamar a fn topsort na tabela:
                    - Add a tabela ao set visited
                    - Loop: P/ todas as dependências da tabela:
                        - Se a dependencia não está no visited:
                            - Chamar fn topsort na dependencia
                    - Adiciona a tabela na pilha
        - Implementação não recursiva: (Suporta nós desconexos no grafo)
            - Pilha de saída
            - Pilha de trabalho
            - Lista de todas as tabelas
            - Lista de visitados
            - Armazena o nº de tabelas
            - Indice do total de tabelas I
            - Loop:
                - Se a pilha de trabalho estiver vazia:
                    - Se nº de visitados == nº de tabelas
                        - Sai do loop. Concluído.
                    - Add tabela[I] no topo da pilha de trabalho
                    - I++
                - Se a tabela do topo da pilha não está nos visitados:
                    - Adiciona a tabela aos visitados
                    - Adiciona a tabela a pilha de saída
                    - Adiciona ao topo da pilha de trabalho as tabelas referenciadas não visitadas
                        - Continua o loop

                - Se está nos visitados:
                    - Remove a pilha de trabalho
            - Imprime a pilha de saída



https://www.geeksforgeeks.org/find-the-ordering-of-tasks-from-given-dependencies/