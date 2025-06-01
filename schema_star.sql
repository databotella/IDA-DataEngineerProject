-- IDP: Recriação idempotente do Data Mart IDA

-- Drop e recria o banco
DROP DATABASE IF EXISTS idadatamart;
CREATE DATABASE idadatamart;
\connect idadatamart

-- Drop e recria schema
DROP SCHEMA IF EXISTS ida CASCADE;
CREATE SCHEMA ida;
SET search_path TO ida, public;

-- ====================================================================
-- DIMENSÕES
-- ====================================================================

-- Dimensão Tempo
DROP TABLE IF EXISTS dim_tempo CASCADE;
CREATE TABLE dim_tempo (
    tempo_key   INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ano_mes     DATE NOT NULL UNIQUE,
    ano         INTEGER NOT NULL,
    mes         INTEGER NOT NULL CHECK (mes BETWEEN 1 AND 12),
    mes_nome    VARCHAR(20) NOT NULL,
    trimestre   INTEGER NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
    semestre    INTEGER NOT NULL CHECK (semestre BETWEEN 1 AND 2)
);
DROP INDEX IF EXISTS idx_dim_tempo_ano_mes;
DROP INDEX IF EXISTS idx_dim_tempo_ano;
CREATE UNIQUE INDEX idx_dim_tempo_ano_mes ON dim_tempo(ano_mes);
CREATE INDEX idx_dim_tempo_ano ON dim_tempo(ano);

-- Dimensão Grupo Econômico
DROP TABLE IF EXISTS dim_grupo_economico CASCADE;
CREATE TABLE dim_grupo_economico (
    grupo_key          INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    grupo_codigo       VARCHAR(20) NOT NULL UNIQUE,
    grupo_nome         VARCHAR(100) NOT NULL,
    grupo_normalizado  VARCHAR(50) NOT NULL,
    ativo              BOOLEAN NOT NULL DEFAULT true
);
DROP INDEX IF EXISTS idx_grupo_codigo;
DROP INDEX IF EXISTS idx_grupo_normalizado;
CREATE UNIQUE INDEX idx_grupo_codigo ON dim_grupo_economico(grupo_codigo);
CREATE INDEX idx_grupo_normalizado ON dim_grupo_economico(grupo_normalizado);

-- Dimensão Serviço
DROP TABLE IF EXISTS dim_servico CASCADE;
CREATE TABLE dim_servico (
    servico_key      INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    servico_codigo   VARCHAR(10) NOT NULL UNIQUE,
    servico_nome     VARCHAR(100) NOT NULL,
    servico_descricao TEXT,
    ativo            BOOLEAN NOT NULL DEFAULT true
);
DROP INDEX IF EXISTS idx_servico_codigo;
CREATE UNIQUE INDEX idx_servico_codigo ON dim_servico(servico_codigo);

-- Dimensão Variável
DROP TABLE IF EXISTS dim_variavel CASCADE;
CREATE TABLE dim_variavel (
    variavel_key      INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    variavel_codigo   VARCHAR(30) NOT NULL UNIQUE,
    variavel_nome     VARCHAR(150) NOT NULL,
    variavel_descricao TEXT,
    unidade_medida    VARCHAR(20),
    peso_ida          INTEGER,
    meta_anatel       NUMERIC(10,3),
    is_principal      BOOLEAN NOT NULL DEFAULT false,
    ativo             BOOLEAN NOT NULL DEFAULT true
);
DROP INDEX IF EXISTS idx_variavel_codigo;
DROP INDEX IF EXISTS idx_variavel_principal;
CREATE UNIQUE INDEX idx_variavel_codigo ON dim_variavel(variavel_codigo);
CREATE INDEX idx_variavel_principal ON dim_variavel(is_principal);

-- ====================================================================
-- FATO
-- ====================================================================

DROP TABLE IF EXISTS fact_ida CASCADE;
CREATE TABLE fact_ida (
    fato_key        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tempo_key       INTEGER NOT NULL REFERENCES dim_tempo(tempo_key),
    grupo_key       INTEGER NOT NULL REFERENCES dim_grupo_economico(grupo_key),
    servico_key     INTEGER NOT NULL REFERENCES dim_servico(servico_key),
    variavel_key    INTEGER NOT NULL REFERENCES dim_variavel(variavel_key),
    valor           NUMERIC(15,6) NOT NULL,
    arquivo_origem  VARCHAR(100) NOT NULL,
    linha_origem    INTEGER,
    data_processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    hash_registro   VARCHAR(64) NOT NULL,
    CONSTRAINT uk_fact_ida_unique UNIQUE (tempo_key, grupo_key, servico_key, variavel_key),
    CONSTRAINT uk_fact_ida_hash UNIQUE (hash_registro)
);
DROP INDEX IF EXISTS idx_fact_ida_metrica_principal;
DROP INDEX IF EXISTS idx_fact_ida_tempo_grupo;
DROP INDEX IF EXISTS idx_fact_ida_arquivo;
CREATE INDEX idx_fact_ida_metrica_principal ON fact_ida(variavel_key, tempo_key, grupo_key);
CREATE INDEX idx_fact_ida_tempo_grupo ON fact_ida(tempo_key, grupo_key);
CREATE INDEX idx_fact_ida_arquivo ON fact_ida(arquivo_origem);

-- ====================================================================
-- VIEW PRINCIPAL
-- ====================================================================

DROP VIEW IF EXISTS vw_taxa_variacao;
CREATE OR REPLACE VIEW vw_taxa_variacao AS
WITH media_mensal AS (
    SELECT
        TO_CHAR(t.ano_mes,'YYYY-MM') AS mes,
        AVG(fi.valor) AS valor_medio,
        LAG(AVG(fi.valor)) OVER (ORDER BY TO_CHAR(t.ano_mes,'YYYY-MM')) AS valor_anterior
    FROM ida.fact_ida fi
    JOIN ida.dim_variavel v ON fi.variavel_key = v.variavel_key
    JOIN ida.dim_tempo t     ON fi.tempo_key    = t.tempo_key
    WHERE v.variavel_codigo = 'TAXA_RESP_5DIAS'
    GROUP BY TO_CHAR(t.ano_mes,'YYYY-MM')
),
media_calc AS (
    SELECT
        mes,
        ROUND(((valor_medio - valor_anterior) / NULLIF(valor_anterior, 0) * 100)::numeric,2) AS taxa_variacao_media
    FROM media_mensal
    WHERE valor_anterior IS NOT NULL
),
grupo_mensal AS (
    SELECT
        g.grupo_codigo,
        TO_CHAR(t.ano_mes,'YYYY-MM') AS mes,
        AVG(fi.valor) AS valor_group,
        LAG(AVG(fi.valor)) OVER (PARTITION BY g.grupo_codigo ORDER BY TO_CHAR(t.ano_mes,'YYYY-MM')) AS valor_anterior
    FROM ida.fact_ida fi
    JOIN ida.dim_variavel v ON fi.variavel_key = v.variavel_key
    JOIN ida.dim_tempo t     ON fi.tempo_key    = t.tempo_key
    JOIN ida.dim_grupo_economico g ON fi.grupo_key = g.grupo_key
    WHERE v.variavel_codigo = 'TAXA_RESP_5DIAS'
    GROUP BY g.grupo_codigo, TO_CHAR(t.ano_mes,'YYYY-MM')
),
grupo_calc AS (
    SELECT
        grupo_codigo,
        mes,
        ROUND(((valor_group - valor_anterior) / NULLIF(valor_anterior, 0) * 100)::numeric,2) AS variacao
    FROM grupo_mensal
    WHERE valor_anterior IS NOT NULL
)
SELECT
    m.mes,
    m.taxa_variacao_media,
    ROUND(MAX(CASE WHEN gc.grupo_codigo='ALGAR' THEN gc.variacao END) - m.taxa_variacao_media,2) AS algar,
    ROUND(MAX(CASE WHEN gc.grupo_codigo='CLARO' THEN gc.variacao END) - m.taxa_variacao_media,2) AS claro,
    ROUND(MAX(CASE WHEN gc.grupo_codigo='VIVO'  THEN gc.variacao END) - m.taxa_variacao_media,2) AS vivo,
    ROUND(MAX(CASE WHEN gc.grupo_codigo='TIM'   THEN gc.variacao END) - m.taxa_variacao_media,2) AS tim,
    ROUND(MAX(CASE WHEN gc.grupo_codigo='OI'    THEN gc.variacao END) - m.taxa_variacao_media,2) AS oi,
    ROUND(MAX(CASE WHEN gc.grupo_codigo='NET'   THEN gc.variacao END) - m.taxa_variacao_media,2) AS net,
    ROUND(MAX(CASE WHEN gc.grupo_codigo='SKY'   THEN gc.variacao END) - m.taxa_variacao_media,2) AS sky,
    ROUND(MAX(CASE WHEN gc.grupo_codigo='EMBRATEL' THEN gc.variacao END) - m.taxa_variacao_media,2) AS embratel,
    ROUND(MAX(CASE WHEN gc.grupo_codigo='NEXTEL'  THEN gc.variacao END) - m.taxa_variacao_media,2) AS nextel,
    ROUND(MAX(CASE WHEN gc.grupo_codigo='SERCOMTEL' THEN gc.variacao END) - m.taxa_variacao_media,2) AS sercomtel
FROM media_calc m
JOIN grupo_calc gc USING (mes)
GROUP BY m.mes, m.taxa_variacao_media
ORDER BY m.mes;

-- Documentação da view `vw_taxa_variacao`
COMMENT ON VIEW ida.vw_taxa_variacao IS
  'View que apresenta a taxa de variação mensal da Taxa de Respondidas em 5 dias úteis, ' ||
  'calculada como ((valor atual – valor anterior)/valor anterior)*100, junto com a média ' ||
  'mensal (taxa_variacao_media) e as diferenças individuais por grupo econômico.';
COMMENT ON COLUMN ida.vw_taxa_variacao.mes IS
  'Mês de referência no formato YYYY-MM';
COMMENT ON COLUMN ida.vw_taxa_variacao.taxa_variacao_media IS
  'Média da variação percentual mensal arredondada a 2 casas';
COMMENT ON COLUMN ida.vw_taxa_variacao.algar IS
  'Diferença entre a variação de ALGAR e a taxa_variacao_media';
COMMENT ON COLUMN ida.vw_taxa_variacao.claro IS
  'Diferença entre a variação de CLARO e a taxa_variacao_media';
COMMENT ON COLUMN ida.vw_taxa_variacao.vivo IS
  'Diferença entre a variação de VIVO e a taxa_variacao_media';
COMMENT ON COLUMN ida.vw_taxa_variacao.tim IS
  'Diferença entre a variação de TIM e a taxa_variacao_media';
COMMENT ON COLUMN ida.vw_taxa_variacao.oi IS
  'Diferença entre a variação de OI e a taxa_variacao_media';
COMMENT ON COLUMN ida.vw_taxa_variacao.net IS
  'Diferença entre a variação de NET e a taxa_variacao_media';
COMMENT ON COLUMN ida.vw_taxa_variacao.sky IS
  'Diferença entre a variação de SKY e a taxa_variacao_media';
COMMENT ON COLUMN ida.vw_taxa_variacao.embratel IS
  'Diferença entre a variação de EMBRATEL e a taxa_variacao_media';
COMMENT ON COLUMN ida.vw_taxa_variacao.nextel IS
  'Diferença entre a variação de NEXTEL e a taxa_variacao_media';
COMMENT ON COLUMN ida.vw_taxa_variacao.sercomtel IS
  'Diferença entre a variação de SERCOMTEL e a taxa_variacao_media';
-- ====================================================================
-- DADOS INICIAIS
-- ====================================================================

INSERT INTO dim_tempo (ano_mes, ano, mes, mes_nome, trimestre, semestre) VALUES
('2017-01-01',2017,1,'Janeiro',1,1),
('2017-02-01',2017,2,'Fevereiro',1,1),
('2017-03-01',2017,3,'Março',1,1),
('2017-04-01',2017,4,'Abril',2,1),
('2017-05-01',2017,5,'Maio',2,1),
('2017-06-01',2017,6,'Junho',2,1),
('2017-07-01',2017,7,'Julho',3,2),
('2017-08-01',2017,8,'Agosto',3,2),
('2017-09-01',2017,9,'Setembro',3,2),
('2017-10-01',2017,10,'Outubro',4,2),
('2017-11-01',2017,11,'Novembro',4,2),
('2017-12-01',2017,12,'Dezembro',4,2),
('2018-01-01',2018,1,'Janeiro',1,1),
('2018-02-01',2018,2,'Fevereiro',1,1),
('2018-03-01',2018,3,'Março',1,1),  
('2018-04-01',2018,4,'Abril',2,1),  
('2018-05-01',2018,5,'Maio',2,1),  
('2018-06-01',2018,6,'Junho',2,1),  
('2018-07-01',2018,7,'Julho',3,2),  
('2018-08-01',2018,8,'Agosto',3,2),  
('2018-09-01',2018,9,'Setembro',3,2),  
('2018-10-01',2018,10,'Outubro',4,2),  
('2018-11-01',2018,11,'Novembro',4,2),  
('2018-12-01',2018,12,'Dezembro',4,2),  
('2019-01-01',2019,1,'Janeiro',1,1),  
('2019-02-01',2019,2,'Fevereiro',1,1),  
('2019-03-01',2019,3,'Março',1,1),  
('2019-04-01',2019,4,'Abril',2,1),  
('2019-05-01',2019,5,'Maio',2,1),  
('2019-06-01',2019,6,'Junho',2,1),  
('2019-07-01',2019,7,'Julho',3,2),  
('2019-08-01',2019,8,'Agosto',3,2),  
('2019-09-01',2019,9,'Setembro',3,2),  
('2019-10-01',2019,10,'Outubro',4,2),  
('2019-11-01',2019,11,'Novembro',4,2),  
('2019-12-01',2019,12,'Dezembro',4,2)
ON CONFLICT (ano_mes) DO NOTHING;

INSERT INTO dim_grupo_economico (grupo_codigo, grupo_nome, grupo_normalizado) VALUES
('ALGAR','ALGAR TELECOM S/A','ALGAR'),
('CLARO','CLARO S.A.','CLARO'),
('VIVO','TELEFÔNICA BRASIL S.A.','VIVO'),
('TIM','TIM S.A.','TIM'),
('OI','OI S.A.','OI'),
('NET','NET SERVIÇOS DE COMUNICAÇÃO S.A.','NET'),
('SKY','SKY BRASIL SERVIÇOS LTDA.','SKY'),
('EMBRATEL','EMPRESA BRASILEIRA DE TELECOMUNICAÇÕES S.A.','EMBRATEL'),
('NEXTEL','NEXTEL TELECOMUNICAÇÕES LTDA.','NEXTEL'),
('SERCOMTEL','SERCOMTEL S/A TELECOMUNICAÇÕES','SERCOMTEL')
ON CONFLICT (grupo_codigo) DO NOTHING;

INSERT INTO dim_servico (servico_codigo, servico_nome, servico_descricao) VALUES
('SMP','Serviço Móvel Pessoal','Telefonia móvel celular'),
('STFC','Serviço Telefônico Fixo Comutado','Telefonia fixa'),
('SCM','Serviço de Comunicação Multimídia','Banda larga fixa')
ON CONFLICT (servico_codigo) DO NOTHING;

INSERT INTO dim_variavel (variavel_codigo, variavel_nome, unidade_medida, is_principal) VALUES
('IDA','Indicador de Desempenho no Atendimento (IDA)','pontos',false),
('INDICE_RECL','Índice de Reclamações','por mil',false),
('QTD_ACESSOS','Quantidade de acessos em serviço','unidades',false),
('QTD_REABERTAS','Quantidade de reabertas','unidades',false),
('QTD_RECLAMACOES','Quantidade de reclamações','unidades',false),
('QTD_RECL_PERIODO','Quantidade de Reclamações no Período','unidades',false),
('QTD_RESPONDIDAS','Quantidade de Respondidas','unidades',false),
('QTD_RESP_5DIAS','Quantidade de Sol. Respondidas em até 5 dias','unidades',false),
('TAXA_REABERTAS','Taxa de Reabertas','percentual',false),
('TAXA_RESP_5DIAS','Taxa de Respondidas em 5 dias Úteis','percentual',true),
('TAXA_RESP_PERIODO','Taxa de Respondidas no Período','percentual',false)
ON CONFLICT (variavel_codigo) DO NOTHING;
