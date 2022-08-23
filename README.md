### Author: Kelvin Carvalho Bomfim

## Projeto de ETL e Consumo de Dados com PySpark e Delta Lake
* Ingestão e normalização dos dados (disposto em Delta Lake de 3 camadas, Bronze, Silver e Gold).
* Enriquecimento dos dados com consumo de API.
* Transformação e agregação dos dados. 
* Uso dos dados. 
* Dockerização do ambiente. 

## Código e recursos utilizados 
**Python Version:** 3.9                                                                                   
**Libs:** os, delta-spark, pyspark, shutil, json, requests , pandas, matplotlib, datetime                                                        
**SPARK Version:** 3.2.0                                                         
**Delta Lake Version:** Delta-Core 2.12                                                         
**API's Consumidas:** https://ipwhois.io                                                      
**Java SDK:** 8                                                                                                           


## Notebooks:
Dentro deste repositório adicionei os notebooks de ETL e Consumo de Dados, para execução dos mesmos é recomendada a utilização da imagem Docker disposta aqui.  
Como inseri na Imagem Docker também as camadas de dados já processados, caso queira executar o projeto do zero, atente-se a primeira célula no notebook "ETL Finalizada" que excluirá os dados prévios para que a execução seja sucedida. (Pode ocorrer lentidão na etapa de consumo da API devido ao tempo de request e iteração) 

## Imagem Docker: https://hub.docker.com/r/kelvincb/pyspark_deltalake
Para fins de maior compatibilidade entre ambientes, criei uma imagem docker contendo tanto o ambiente utilizado durante o desenvolvimento quanto o projeto em si.  
Para instalar basta executar o Docker Pull Command:

```bash
docker pull kelvincb/pyspark_deltalake:v1
```

## ETL + DELTA LAKE:  
Esta ETL foi desenvolvida com o propósito de refinar e enriquecer um dataset de pedidos de um determinado e-commerce genérico. Para tal se viu necessário tratar as seguintes colunas:  

**- nome e sobrenome:** Concatenadas em uma única coluna contendo o nome completo do cliente  
**- valor_pedido:** Filtrada de caracteres indesejados e realizado o cast para tipo Double  
**- data_pedido:** Realizado o cast para tipo TimeStamp  

Todas as alterações feitas foram salvas como parquets em layes do Delta Lake  

Quanto ao enriquecimento dos dados, uma API de localização por IP (IpWhois) foi consumida com um dataset complementar contendo IP's dos clientes. Para tal as seguintes modificações ocorreram:  

**- Definição de função para consumo da API.**    
**- Abertura de 3 novas colunas no dataset de IP's:** regiao, cidade e pais.  
**- Join dos dataframes de IP**  
**- Remoção da coluna contendo endereços IP's**  
**- Agragação dos dados do dataframe de endereços e do dataframe principal dos clientes:** Inner-Join com referencia ao id_ip  

## Consumo dos Dados:
Com os dados tratados, enriquecidos e refinados, um exemplo de consumo dos mesmos foi feito.
A primeira analise feita foi referente a quantidade de compras feitas por tipo de bandeira de cartão de crédito, plotando em bar-graph a quantidade de compras feitas por cada bandeira e denotando as duas mais populares em território brasileiro: Visa e MasterCard.

![Alt text](/Imagens/Compras_bandeira.png?raw=true "Plot de compras por bandeira de cartão")

A segunda analise feita, foi quanto ao volume de compras de acordo com o pais de origem, elencando possiveis paises de interesse para promoções personalizadas

![Alt text](/Imagens/Compras_paises.png?raw=true "Plot de volumes de compra por pais")

A terceira analise feita, já utiliza dos dados obtidos anteriormente, dessa vez selecionando apenas as compras feitas em território estadunidense para verificar quais meses do ano possuem maior volume de compras, assim podendo criar estratégias especificas para cada temporada.

![Alt text](/Imagens/Compras_mes_eua.png?raw=true "Plot meses com maior volume de compra nos eua")

Apesar de simples, os tratamentos, enriquecimentos e analises feitas demonstram bem a capacidade que as ferramentas utilizadas tem para manipulação de dados, podendo agregar imenso valor a diversos setores da economia e gestão.


