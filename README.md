# Pipeline_Movie

# Visão Geral

O MoviePulse Analytics é uma plataforma de dados desenvolvida para monitorar, consolidar e analisar, de forma contínua, o desempenho e as tendências do mercado cinematográfico a partir de dados públicos da API do The Movie Database (TMDb).

O projeto foi concebido com foco em engenharia de dados, analytics e entrega de valor ao negócio, simulando um cenário real de tomada de decisão orientada por dados no setor de entretenimento.


## Problema de Negócio

Empresas do setor de mídia, streaming e marketing enfrentam desafios para:

Identificar rapidamente tendências emergentes de consumo de conteúdo;

Entender se a popularidade de um filme está relacionada à qualidade percebida ou apenas ao hype momentâneo;

Acompanhar a evolução diária de indicadores como popularidade, avaliações e engajamento;

Disponibilizar informações confiáveis e atualizadas para áreas como produto, marketing e planejamento estratégico.

Essas análises exigem dados atualizados, estruturados, históricos e de fácil acesso, o que raramente é entregue de forma integrada por fontes externas.


## Solução Proposta

O MoviePulse Analytics resolve esse problema por meio de um pipeline de dados automatizado, capaz de:

Coletar diariamente dados de filmes em alta;

Enriquecer informações com detalhes técnicos, gêneros, produtoras e métricas de engajamento;

Armazenar dados históricos de forma estruturada;

Processar e consolidar KPIs analíticos;

Disponibilizar resultados via API e consultas analíticas de alta performance.

A solução foi projetada seguindo boas práticas de arquitetura de dados moderna, separando ingestão, processamento, armazenamento e entrega.


## Indicadores Estratégicos (KPIs)

Top filmes por popularidade (diário e semanal);

Variação de popularidade ao longo do tempo (momentum);

Participação de gêneros no ranking de filmes populares;

Relação entre avaliação média, número de votos e popularidade;

Distribuição de produtoras entre os filmes de maior destaque.


# Arquiteura do Projeto;

<img width="1023" height="707" alt="image" src="https://github.com/user-attachments/assets/3ce31a22-e4a2-4c57-995e-0cbec3094fb2" />

Essa arquitetura implementa um pipeline orientado a eventos, onde dados de mercado cinematográfico são ingeridos via Kafka, persistidos em uma camada histórica, processados com Spark e disponibilizados em camadas analíticas que atendem diferentes áreas do negócio, como Marketing e BI, com observabilidade e governança.






