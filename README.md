### Author: Kelvin Carvalho Bomfim

## Projeto de ETL e Consumo de Dados com PySpark e Delta Lake
* Ingestão e normalização dos dados (disposto em Delta Lake de 3 camadas, Bronze, Silver e Gold).
* Enriquecimento dos dados com consumo de API.
* Transformação e agregação dos dados. 
* Uso dos dados. 
* Dockerização do ambiente. 

## Código e recursos utilizados 
**Python Version:** 3.9                                                                                   
**Libs:** os, delta-spark, pyspark, shutil, json, requests                                                         
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



