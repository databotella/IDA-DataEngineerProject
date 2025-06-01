#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL IDA - Sistema de Extração, Transformação e Carga de Dados
=============================================================

Este módulo implementa o processo ETL para carregar dados do Índice de 
Desempenho no Atendimento (IDA) da ANATEL no Data Mart PostgreSQL.

O sistema baixa e processa arquivos ODS diretamente na memória,
sem necessidade de armazenamento em disco.

Classes:
    BaseExtractor: Classe base para extração de dados
    ODSExtractor: Extrator especializado para arquivos ODS
    DataTransformer: Transformador de dados para formato normalizado
    PostgreSQLLoader: Carregador de dados no PostgreSQL
    ETLPipeline: Orquestrador do pipeline ETL com download via API

Exemplo de uso:
    >>> pipeline = ETLPipeline()
    >>> pipeline.run()

Autor: beAnalytic Team
Data: 2024
Versão: 2.0.0
"""

import os
import sys
import logging
import hashlib
import requests
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from pathlib import Path
from io import BytesIO

import pandas as pd
import numpy as np
import psycopg2
import re
from psycopg2.extras import execute_values
from psycopg2.pool import SimpleConnectionPool


# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_ida.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Define o nível de log via variável de ambiente
log_level = os.environ.get('LOG_LEVEL', 'INFO')
logging.getLogger().setLevel(getattr(logging, log_level))


@dataclass
class ETLConfig:
    """Configuração do ETL com parâmetros de conexão e arquivos.
    
    Attributes:
        db_connection_string: String de conexão PostgreSQL
        batch_size: Tamanho do lote para inserção em batch
        max_retries: Número máximo de tentativas em caso de erro
        api_key: Chave de API para dados.gov.br
    """
    db_connection_string: str = "postgresql://postgres:postgres@postgres:5432/idadatamart"
    batch_size: int = 1000
    max_retries: int = 3
    api_key: str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJVZ0lfQkVMbkdHakM3eGl5d3pOQWRfWHRDODdaX0NXQmdNQnBObVB0ZktBeW85QmRVZGdTemF6N1hmT2tEako4cGlQQTVtaUViYmpQNEFYOSIsImlhdCI6MTc0ODcwMjM3N30.w-TGAteMgx2tW8O-UO_1XWjR4TZMgqAtFMbvWeE1VUo"


@dataclass
class RecursoIDA:
    """Dados de um recurso IDA da ANATEL."""
    id: str
    titulo: str
    url: str
    formato: str
    ano: int
    servico: str


@dataclass
class DataRecord:
    """Registro de dados normalizado para carga no banco.
    
    Attributes:
        ano_mes: Data no formato YYYY-MM-01
        grupo_economico: Código do grupo econômico
        servico: Código do serviço (SMP, SCM, STFC)
        variavel: Código da variável
        valor: Valor da métrica
        arquivo_origem: Nome do arquivo de origem
        linha_origem: Número da linha no arquivo
    """
    ano_mes: str
    grupo_economico: str
    servico: str
    variavel: str
    valor: float
    arquivo_origem: str
    linha_origem: int
    
    def generate_hash(self) -> str:
        """Gera hash MD5 único para o registro.
        
        Returns:
            str: Hash MD5 do registro
        """
        content = f"{self.ano_mes}|{self.grupo_economico}|{self.servico}|{self.variavel}|{self.valor}"
        return hashlib.md5(content.encode()).hexdigest()


class BaseExtractor(ABC):
    """Classe base abstrata para extratores de dados.
    
    Define a interface para extratores especializados que processam
    diferentes formatos de arquivo.
    """
    
    def __init__(self, config: ETLConfig):
        """Inicializa o extrator com configuração.
        
        Args:
            config: Configuração do ETL
        """
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def extract(self, file_content: BytesIO, metadata: Dict[str, Any]) -> pd.DataFrame:
        """Extrai dados do conteúdo do arquivo.
        
        Args:
            file_content: Conteúdo do arquivo em memória
            metadata: Metadados do arquivo (nome, serviço, etc)
            
        Returns:
            pd.DataFrame: Dados extraídos
        """
        pass


class ODSExtractor(BaseExtractor):
    """Extrator especializado para arquivos ODS (OpenDocument Spreadsheet).
    
    Processa arquivos ODS do portal de dados abertos da ANATEL,
    extraindo dados das planilhas específicas de cada serviço.
    """
    
    # Mapeamento de sheets por serviço
    SHEET_MAPPING = {
        'SMP': 'Móvel_Pessoal',
        'SCM': 'Banda_Larga_Fixa', 
        'STFC': 'Telefonia_Fixa'
    }
    
    # Mapeamento de nomes de variáveis para códigos padronizados
    VARIABLE_MAPPING = {
        'Indicador de Desempenho no Atendimento (IDA)': 'IDA',
        'Índice de Reclamações': 'INDICE_RECL',
        'Quantidade de acessos em serviço': 'QTD_ACESSOS',
        'Quantidade de reabertas': 'QTD_REABERTAS',
        'Quantidade de reclamações': 'QTD_RECLAMACOES',
        'Quantidade de Reclamações no Período': 'QTD_RECL_PERIODO',
        'Quantidade de Respondidas': 'QTD_RESPONDIDAS',
        'Quantidade de Sol. Respondidas em até 5 dias': 'QTD_RESP_5DIAS',
        'Quantidade de Sol. Respondidas no Período': 'QTD_RESP_PERIODO',
        'Taxa de Reabertas': 'TAXA_REABERTAS',
        'Taxa de Respondidas em 5 dias Úteis': 'TAXA_RESP_5DIAS',
        'Taxa de Respondidas no Período': 'TAXA_RESP_PERIODO'
    }
    
    def extract(self, file_content: BytesIO, metadata: Dict[str, Any]) -> pd.DataFrame:
        """Extrai dados de um arquivo ODS em memória.
        
        Args:
            file_content: Conteúdo do arquivo ODS em memória
            metadata: Dicionário com 'filename', 'servico'
            
        Returns:
            pd.DataFrame: Dados extraídos com colunas padronizadas
        """
        servico = metadata['servico']
        filename = metadata['filename']
        sheet_name = self.SHEET_MAPPING.get(servico)
        
        if not sheet_name:
            raise ValueError(f"Serviço {servico} não tem mapeamento de sheet")
        
        self.logger.info(f"Extraindo dados de {filename} - Sheet: {sheet_name}")
        
        try:
            # Lê o arquivo sem header para analisar a estrutura
            df_raw = pd.read_excel(
                file_content,
                sheet_name=sheet_name,
                header=None,
                engine='odf'
            )
            
            # Volta ao início do arquivo para próxima leitura
            file_content.seek(0)
            
            # Encontra a linha de cabeçalho
            header_row = self._find_header_row(df_raw)
            self.logger.info(f"Linha de cabeçalho encontrada: {header_row}")
            
            # Lê novamente com o header correto
            df = pd.read_excel(
                file_content,
                sheet_name=sheet_name,
                header=header_row,
                engine='odf'
            )
            
            # Renomeia as primeiras colunas
            columns = df.columns.tolist()
            if len(columns) >= 2:
                columns[0] = 'GRUPO_ECONOMICO'
                columns[1] = 'VARIAVEL'
                df.columns = columns
            
            # Adiciona informações do serviço como atributos
            df['SERVICO'] = servico
            df['ARQUIVO_ORIGEM'] = filename
            
            # Preenche grupos faltantes (repetidos no arquivo)
            df['GRUPO_ECONOMICO'] = df['GRUPO_ECONOMICO'].ffill()
            
            self.logger.info(f"Extraídos {len(df)} registros de {filename}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Erro ao extrair dados: {str(e)}")
            raise
    
    def _find_header_row(self, df_raw: pd.DataFrame) -> int:
        """Encontra a linha de cabeçalho no DataFrame."""
        for idx in range(min(20, len(df_raw))):
            row_values = df_raw.iloc[idx].astype(str)
            normalized = [str(v).strip().upper() for v in row_values]
            
            # identifica cabeçalho por colunas de texto
            if any('GRUPO' in v for v in normalized) and any('VARIAVEL' in v for v in normalized):
                return idx
            
            # identifica cabeçalho por padrão YYYY-MM
            if any(re.search(r'\d{4}-\d{2}', v) for v in normalized):
                return idx
        
        # Valor padrão
        return 8


class DataTransformer:
    """Transformador de dados para formato normalizado.
    
    Realiza a transformação dos dados extraídos para o formato
    normalizado esperado pelo banco de dados.
    """
    
    def __init__(self, config: ETLConfig):
        """Inicializa o transformador.
        
        Args:
            config: Configuração do ETL
        """
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def transform(self, df: pd.DataFrame) -> List[DataRecord]:
        """Transforma DataFrame em lista de registros normalizados.
        
        Args:
            df: DataFrame com dados extraídos
            
        Returns:
            List[DataRecord]: Lista de registros normalizados
        """
        self.logger.info("Iniciando transformação dos dados")
        self.logger.info(f"Colunas encontradas: {df.columns.tolist()}")
        
        records = []
        
        # Identifica colunas de meses
        month_columns = [col for col in df.columns if self._is_month_column(col)]
        self.logger.info(f"Colunas de meses identificadas: {len(month_columns)} colunas")
        
        if not month_columns:
            self.logger.error("Nenhuma coluna de mês encontrada!")
            return records
        
        # Processa cada linha do DataFrame
        for idx, row in df.iterrows():
            # Na estrutura real, GRUPO está na primeira coluna e VARIÁVEL na segunda
            grupo = str(row.iloc[0]) if not pd.isna(row.iloc[0]) else ""
            variavel = str(row.iloc[1]) if not pd.isna(row.iloc[1]) else ""
            
            # Remove 'nan' e espaços
            grupo = grupo.strip() if grupo != 'nan' else ""
            variavel = variavel.strip() if variavel != 'nan' else ""
            
            # Pula linhas sem grupo ou variável
            if not grupo or not variavel:
                continue
            
            # Limpa e padroniza o grupo
            grupo_limpo = self._clean_text(grupo)
            variavel_limpa = self._clean_text(variavel)
            
            # Mapeia variável para código
            variavel_codigo = ODSExtractor.VARIABLE_MAPPING.get(variavel_limpa, variavel_limpa)
            
            # Processa cada mês
            for month_col in month_columns:
                try:
                    valor = row[month_col]
                    
                    # Pula valores vazios ou inválidos
                    if pd.isna(valor) or str(valor).strip() in ['-', '', 'nan']:
                        continue
                    
                    # Converte valor para float
                    valor_float = self._parse_value(valor)
                    if valor_float is None:
                        continue
                    
                    # Formata a data corretamente
                    if hasattr(month_col, 'strftime'):
                        # É um Timestamp
                        ano_mes = month_col.strftime('%Y-%m-01')
                    else:
                        # É uma string
                        month_str = str(month_col).strip()
                        if ' ' in month_str:  # Remove parte da hora se existir
                            month_str = month_str.split(' ')[0]
                        ano_mes = f"{month_str}-01"
                    
                    # Cria registro normalizado
                    record = DataRecord(
                        ano_mes=ano_mes,
                        grupo_economico=grupo_limpo,
                        servico=row['SERVICO'],
                        variavel=variavel_codigo,
                        valor=valor_float,
                        arquivo_origem=row['ARQUIVO_ORIGEM'],
                        linha_origem=idx
                    )
                    
                    records.append(record)
                    
                except Exception as e:
                    self.logger.debug(f"Erro ao processar célula: {str(e)}")
        
        self.logger.info(f"Transformados {len(records)} registros")
        return records
    
    def _is_month_column(self, col: str) -> bool:
        """Verifica se a coluna é um mês no formato YYYY-MM."""
        try:
            col_str = str(col).strip()
            
            # Verifica formato YYYY-MM
            if len(col_str) == 7 and col_str[4] == '-':
                year, month = col_str.split('-')
                return 2000 <= int(year) <= 2030 and 1 <= int(month) <= 12
            
            # Verifica se é um Timestamp pandas
            if hasattr(col, 'year') and hasattr(col, 'month'):
                return True
                
            return False
        except:
            return False
    
    def _clean_text(self, text: Any) -> str:
        """Limpa e padroniza texto."""
        if pd.isna(text):
            return ""
        
        text = str(text).strip()
        text = ' '.join(text.split())
        
        # Padroniza grupos econômicos conhecidos
        group_mapping = {
            'ALGAR TELECOM S/A': 'ALGAR',
            'CLARO S.A.': 'CLARO',
            'TELEFÔNICA BRASIL S.A.': 'VIVO',
            'TIM S.A.': 'TIM',
            'OI S.A.': 'OI',
            'NET SERVIÇOS DE COMUNICAÇÃO S.A.': 'NET',
            'SKY BRASIL SERVIÇOS LTDA.': 'SKY',
            'EMPRESA BRASILEIRA DE TELECOMUNICAÇÕES S.A. - EMBRATEL': 'EMBRATEL',
            'NEXTEL TELECOMUNICAÇÕES LTDA.': 'NEXTEL',
            'SERCOMTEL S.A. TELECOMUNICAÇÕES': 'SERCOMTEL'
        }
        
        return group_mapping.get(text, text)
    
    def _parse_value(self, value: Any) -> Optional[float]:
        """Converte valor para float."""
        try:
            if isinstance(value, str):
                value = value.replace(',', '.').replace('%', '')
            return float(value)
        except:
            return None


class PostgreSQLLoader:
    """Carregador de dados no PostgreSQL.
    
    Realiza a carga dos dados transformados no banco de dados.
    """
    
    def __init__(self, config: ETLConfig):
        """Inicializa o carregador.
        
        Args:
            config: Configuração do ETL
        """
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self._connection_pool = None
    
    def __enter__(self):
        """Inicializa pool de conexões."""
        self._connection_pool = SimpleConnectionPool(
            1, 5,
            self.config.db_connection_string
        )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Fecha pool de conexões."""
        if self._connection_pool:
            self._connection_pool.closeall()
    
    def load(self, records: List[DataRecord]) -> int:
        """Carrega registros no banco de dados.
        
        Args:
            records: Lista de registros a serem carregados
            
        Returns:
            int: Número de registros inseridos
        """
        if not records:
            self.logger.warning("Nenhum registro para carregar")
            return 0
        
        conn = self._connection_pool.getconn()
        try:
            with conn.cursor() as cursor:
                self._ensure_dimensions(cursor, records)
                inserted = 0
                
                # Processa em batches
                for i in range(0, len(records), self.config.batch_size):
                    batch = records[i:i + self.config.batch_size]
                    batch_inserted = self._insert_batch(cursor, batch)
                    inserted += batch_inserted
                    
                    self.logger.info(
                        f"Batch {i//self.config.batch_size + 1}: "
                        f"{batch_inserted}/{len(batch)} registros inseridos"
                    )
                
                conn.commit()
                self.logger.info(f"Total de registros inseridos: {inserted}")
                return inserted
                
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Erro ao carregar dados: {str(e)}")
            raise
        finally:
            self._connection_pool.putconn(conn)
    
    def _insert_batch(self, cursor, batch: List[DataRecord]) -> int:
        """Insere um batch de registros."""
        insert_query = """
            INSERT INTO ida.fact_ida (
                tempo_key,
                grupo_key,
                servico_key,
                variavel_key,
                valor,
                arquivo_origem,
                linha_origem,
                hash_registro
            )
            SELECT
                t.tempo_key,
                g.grupo_key,
                s.servico_key,
                v.variavel_key,
                %(valor)s::numeric,
                %(arquivo_origem)s,
                %(linha_origem)s,
                %(hash_registro)s
            FROM
                ida.dim_tempo t,
                ida.dim_grupo_economico g,
                ida.dim_servico s,
                ida.dim_variavel v
            WHERE
                t.ano_mes = %(ano_mes)s::date
                AND g.grupo_codigo = %(grupo_economico)s
                AND s.servico_codigo = %(servico)s
                AND v.variavel_codigo = %(variavel)s
            ON CONFLICT (hash_registro) DO NOTHING
        """
        
        inserted = 0
        for record in batch:
            params = {
                'ano_mes': record.ano_mes,
                'grupo_economico': record.grupo_economico,
                'servico': record.servico,
                'variavel': record.variavel,
                'valor': record.valor,
                'arquivo_origem': record.arquivo_origem,
                'linha_origem': record.linha_origem,
                'hash_registro': record.generate_hash()
            }
            cursor.execute(insert_query, params)
            if cursor.rowcount == 0:
                self.logger.warning(f"Ignorado registro sem correspondência: {params}")
            inserted += cursor.rowcount
        
        return inserted

    def _ensure_dimensions(self, cursor, records: List[DataRecord]) -> None:
        """Garante que todas as entradas de dimensão existam"""
        from datetime import datetime
        
        # Nomes de meses em português
        month_names = {
            1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
            5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
            9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'
        }
        
        # Coleta valores únicos
        tempos = set(r.ano_mes for r in records)
        grupos = set(r.grupo_economico for r in records)
        servicos = set(r.servico for r in records)
        variaveis = set(r.variavel for r in records)

        # Insere dim_tempo
        for ano_mes in tempos:
            dt = datetime.strptime(ano_mes, '%Y-%m-%d')
            year, month = dt.year, dt.month
            mes_nome = month_names[month]
            trimestre = (month - 1) // 3 + 1
            semestre = (month - 1) // 6 + 1
            cursor.execute(
                """
                INSERT INTO ida.dim_tempo (ano_mes, ano, mes, mes_nome, trimestre, semestre)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (ano_mes) DO NOTHING
                """,
                (ano_mes, year, month, mes_nome, trimestre, semestre)
            )

        # Insere dim_grupo_economico
        for grp in grupos:
            cursor.execute(
                """
                INSERT INTO ida.dim_grupo_economico (grupo_codigo, grupo_nome, grupo_normalizado)
                VALUES (%s, %s, %s)
                ON CONFLICT (grupo_codigo) DO NOTHING
                """,
                (grp, grp, grp)
            )

        # Insere dim_servico
        for svc in servicos:
            cursor.execute(
                """
                INSERT INTO ida.dim_servico (servico_codigo, servico_nome, servico_descricao)
                VALUES (%s, %s, %s)
                ON CONFLICT (servico_codigo) DO NOTHING
                """,
                (svc, svc, svc)
            )

        # Insere dim_variavel
        for var in variaveis:
            cursor.execute(
                """
                INSERT INTO ida.dim_variavel (variavel_codigo, variavel_nome)
                VALUES (%s, %s)
                ON CONFLICT (variavel_codigo) DO NOTHING
                """,
                (var, var)
            )


class ETLPipeline:
    """Orquestrador do pipeline ETL completo com download via API.
    
    Baixa e processa arquivos diretamente na memória, sem 
    necessidade de armazenamento em disco.
    """
    
    def check_database_health(self) -> bool:
        """Verifica se o banco está acessível e com schema correto."""
        try:
            conn = psycopg2.connect(self.config.db_connection_string)
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'ida';")
            if not cur.fetchone():
                self.logger.error("Schema 'ida' não encontrado.")
                cur.close()
                conn.close()
                return False
            required_tables = ['dim_tempo', 'dim_grupo_economico', 'fact_ida']
            for table in required_tables:
                cur.execute(f"SELECT 1 FROM ida.{table} LIMIT 1;")
            cur.close()
            conn.close()
            self.logger.info("Database health check passed.")
            return True
        except Exception as e:
            self.logger.error(f"Database health check failed: {e}")
            return False

    # Configurações da API dados.gov.br
    BASE_URL = "https://dados.gov.br/dados/api/publico"
    CONJUNTO_IDA_ID = "63a9c9f6-9991-48b4-a072-ce22765652e6"
    SERVICOS_ALVO = ["SMP", "STFC", "SCM"]
    ANOS_ALVO = [2017, 2018, 2019]
    
    def __init__(self, config: Optional[ETLConfig] = None):
        """Inicializa o pipeline.
        
        Args:
            config: Configuração do ETL (usa padrão se não fornecida)
        """
        self.config = config or ETLConfig()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.headers = {"chave-api-dados-abertos": self.config.api_key}
        self.extractor = ODSExtractor(self.config)
        self.transformer = DataTransformer(self.config)
        self.loader = PostgreSQLLoader(self.config)
    
    def _requisicao_api(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Faz request na API com tratamento de erro."""
        url = f"{self.BASE_URL}/{endpoint}"
        
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.logger.error(f"Erro API {url}: {e}")
            return None
    
    def _extrair_info_recurso(self, recurso_raw: Dict) -> Optional[RecursoIDA]:
        """Extrai informações relevantes do recurso."""
        titulo = recurso_raw.get('titulo', '')
        
        # Busca ano no título
        ano = None
        for a in self.ANOS_ALVO:
            if str(a) in titulo:
                ano = a
                break
        
        # Busca serviço no título  
        servico = None
        for s in self.SERVICOS_ALVO:
            if s in titulo.upper():
                servico = s
                break
        
        if not (ano and servico):
            return None
            
        return RecursoIDA(
            id=recurso_raw.get('id', ''),
            titulo=titulo,
            url=recurso_raw.get('link', '').replace('\\', '/'),
            formato=recurso_raw.get('formato', ''),
            ano=ano,
            servico=servico
        )
    
    def _baixar_arquivo_memoria(self, recurso: RecursoIDA) -> Optional[BytesIO]:
        """Baixa arquivo diretamente para memória."""
        if not recurso.url:
            self.logger.warning(f"URL vazia para recurso: {recurso.titulo}")
            return None
        
        try:
            self.logger.info(f"Baixando para memória: {recurso.titulo}")
            response = requests.get(recurso.url, timeout=60)
            response.raise_for_status()
            
            # Retorna conteúdo em memória
            return BytesIO(response.content)
            
        except Exception as e:
            self.logger.error(f"Erro ao baixar {recurso.titulo}: {e}")
            return None
    
    def run(self) -> Dict[str, Any]:
        """Executa o pipeline ETL completo.
        
        Baixa e processa arquivos diretamente na memória.
        
        Returns:
            Dict[str, Any]: Estatísticas da execução
        """
        self.logger.info("========== INICIANDO PIPELINE ETL IDA ==========")
        start_time = datetime.now()
        
        stats = {
            'recursos_processados': 0,
            'registros_extraidos': 0,
            'registros_transformados': 0,
            'registros_carregados': 0,
            'erros': []
        }
        # Verificação de saúde do banco antes da extração
        self.logger.info("ETAPA 0: Verificando saúde do banco de dados")
        if not self.check_database_health():
            self.logger.error("Falha na verificação de saúde do banco. Abortando pipeline.")
            return stats
        
        # Busca recursos na API
        self.logger.info("ETAPA 1: Consultando recursos na API dados.gov.br")
        dados = self._requisicao_api(f"conjuntos-dados/{self.CONJUNTO_IDA_ID}")
        if not dados:
            self.logger.error("Falha ao obter dados do conjunto IDA")
            return stats
        
        # Filtra recursos relevantes
        recursos_brutos = dados.get('recursos', [])
        recursos_filtrados = []
        
        for recurso_raw in recursos_brutos:
            recurso = self._extrair_info_recurso(recurso_raw)
            if recurso:
                recursos_filtrados.append(recurso)
                self.logger.info(f"Recurso encontrado: {recurso.servico} {recurso.ano}")
        
        self.logger.info(f"Total de recursos para processar: {len(recursos_filtrados)}")
        
        # Processa cada recurso
        self.logger.info("ETAPA 2: Processando recursos")
        all_records = []
        
        with self.loader:
            for recurso in recursos_filtrados:
                try:
                    # Baixa arquivo para memória
                    file_content = self._baixar_arquivo_memoria(recurso)
                    if not file_content:
                        continue
                    
                    # Extrai dados
                    metadata = {
                        'filename': f"{recurso.servico}_{recurso.ano}.ods",
                        'servico': recurso.servico
                    }
                    df = self.extractor.extract(file_content, metadata)
                    stats['registros_extraidos'] += len(df)
                    
                    # Transforma dados
                    records = self.transformer.transform(df)
                    stats['registros_transformados'] += len(records)
                    all_records.extend(records)
                    
                    # Carrega no banco em lotes para não acumular muita memória
                    if len(all_records) >= self.config.batch_size * 5:
                        loaded = self.loader.load(all_records)
                        stats['registros_carregados'] += loaded
                        all_records = []  # Limpa lista
                    
                    stats['recursos_processados'] += 1
                    self.logger.info(f"Recurso {recurso.servico} {recurso.ano}: {len(records)} registros")
                    
                except Exception as e:
                    error_msg = f"Erro ao processar {recurso.titulo}: {str(e)}"
                    self.logger.error(error_msg)
                    stats['erros'].append(error_msg)
            
            # Carrega registros restantes
            if all_records:
                self.logger.info(f"ETAPA 3: Carregando {len(all_records)} registros restantes")
                loaded = self.loader.load(all_records)
                stats['registros_carregados'] += loaded
        
        # Finaliza e reporta estatísticas
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        self.logger.info("========== PIPELINE FINALIZADO ==========")
        self.logger.info(
            f"Duração: {duration:.2f} segundos\n"
            f"Recursos processados: {stats['recursos_processados']}\n"
            f"Registros extraídos: {stats['registros_extraidos']}\n"
            f"Registros transformados: {stats['registros_transformados']}\n"
            f"Registros carregados: {stats['registros_carregados']}\n"
            f"Erros: {len(stats['erros'])}"
        )
        
        return stats


def main():
    """Função principal para execução do ETL.
    
    Exemplo de uso:
        python etl_ida.py
    """
    try:
        # Cria e executa pipeline
        pipeline = ETLPipeline()
        stats = pipeline.run()
        
        # Verifica se houve sucesso
        if stats['registros_carregados'] == 0 and stats['registros_transformados'] > 0:
            logging.error("ATENÇÃO: Dados foram processados mas não foram carregados!")
            logging.error("Verifique se o schema 'ida' existe no banco de dados")
        
        # Retorna código de saída baseado em erros
        if stats['erros']:
            sys.exit(1)
        else:
            sys.exit(0)
            
    except Exception as e:
        logging.error(f"Erro fatal no ETL: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
