#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL IDA - Sistema de Extração, Transformação e Carga de Dados
=============================================================

Este módulo implementa o processo ETL para carregar dados do Índice de 
Desempenho no Atendimento (IDA) da ANATEL no Data Mart PostgreSQL.

O sistema processa arquivos ODS contendo métricas de desempenho das
operadoras de telecomunicações e carrega os dados normalizados na
tabela fact_ida do banco de dados.

Classes:
    BaseExtractor: Classe base para extração de dados
    ODSExtractor: Extrator especializado para arquivos ODS
    DataTransformer: Transformador de dados para formato normalizado
    PostgreSQLLoader: Carregador de dados no PostgreSQL
    ETLPipeline: Orquestrador do pipeline ETL

Exemplo de uso:
    >>> pipeline = ETLPipeline()
    >>> pipeline.run()

Autor: beAnalytic Team
Data: 2024
Versão: 1.0.0
"""

import os
import sys
import logging
import hashlib
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from pathlib import Path

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
        data_path: Caminho para os arquivos de dados
        batch_size: Tamanho do lote para inserção em batch
        max_retries: Número máximo de tentativas em caso de erro
    """
    db_connection_string: str = "postgres://postgres:aPLQOOYuJClt3jdN1BdtC9U8F10zGtwb%40@212.85.21.2:3010/idadatamart"
    data_path: str = "./data"
    batch_size: int = 1000
    max_retries: int = 3


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
    def extract(self, file_path: str) -> pd.DataFrame:
        """Extrai dados do arquivo especificado.
        
        Args:
            file_path: Caminho para o arquivo
            
        Returns:
            pd.DataFrame: Dados extraídos
        """
        pass


class ODSExtractor(BaseExtractor):
    """Extrator especializado para arquivos ODS (OpenDocument Spreadsheet).
    
    Processa arquivos ODS do portal de dados abertos da ANATEL,
    extraindo dados das planilhas específicas de cada serviço.
    """
    
    # Mapeamento de arquivos para sheets e serviços
    FILE_MAPPING = {
        # 2017
        'SMP_2017.ods': {'sheet': 'Móvel_Pessoal', 'service': 'SMP'},
        'SCM_2017.ods': {'sheet': 'Banda_Larga_Fixa', 'service': 'SCM'},
        'STFC_2017.ods': {'sheet': 'Telefonia_Fixa', 'service': 'STFC'},
        # 2018
        'SMP_2018.ods': {'sheet': 'Móvel_Pessoal', 'service': 'SMP'},
        'SCM_2018.ods': {'sheet': 'Banda_Larga_Fixa', 'service': 'SCM'},
        'STFC_2018.ods': {'sheet': 'Telefonia_Fixa', 'service': 'STFC'},
        # 2019
        'SMP_2019.ods': {'sheet': 'Móvel_Pessoal', 'service': 'SMP'},
        'SCM_2019.ods': {'sheet': 'Banda_Larga_Fixa', 'service': 'SCM'},
        'STFC_2019.ods': {'sheet': 'Telefonia_Fixa', 'service': 'STFC'}
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
    
    def extract(self, file_path: str) -> pd.DataFrame:
        """Extrai dados de um arquivo ODS.
        
        Args:
            file_path: Caminho para o arquivo ODS
            
        Returns:
            pd.DataFrame: Dados extraídos com colunas padronizadas
            
        Raises:
            ValueError: Se o arquivo não for encontrado no mapeamento
            FileNotFoundError: Se o arquivo não existir
        """
        filename = os.path.basename(file_path)
        
        if filename not in self.FILE_MAPPING:
            raise ValueError(f"Arquivo {filename} não está no mapeamento conhecido")
        
        mapping = self.FILE_MAPPING[filename]
        self.logger.info(f"Extraindo dados de {file_path} - Sheet: {mapping['sheet']}")
        
        try:
            # Lê o arquivo sem header para analisar a estrutura
            df_raw = pd.read_excel(
                file_path,
                sheet_name=mapping['sheet'],
                header=None,
                engine='odf'
            )
            
            # Encontra a linha de cabeçalho (por texto ou datas)
            header_row = None
            for idx in range(min(20, len(df_raw))):
                row_values = df_raw.iloc[idx].astype(str)
                normalized = [str(v).strip().upper() for v in row_values]
                # identifica cabeçalho por colunas de texto
                if any('GRUPO' in v for v in normalized) and any('VARIAVEL' in v for v in normalized):
                    header_row = idx
                    break
                # identifica cabeçalho por padrão YYYY-MM
                if any(re.search(r'\d{4}-\d{2}', v) for v in normalized):
                    header_row = idx
                    break
            if header_row is None:
                header_row = 8
                self.logger.warning(f"Header não detectado automaticamente, usando linha {header_row}")
            else:
                self.logger.info(f"Linha de cabeçalho encontrada: {header_row}")
            
            # Lê novamente com o header correto
            df = pd.read_excel(
                file_path,
                sheet_name=mapping['sheet'],
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
            df['SERVICO'] = mapping['service']
            df['ARQUIVO_ORIGEM'] = filename
            # Preenche grupos faltantes (repetidos no arquivo)
            df['GRUPO_ECONOMICO'] = df['GRUPO_ECONOMICO'].ffill()
            
            self.logger.info(f"Extraídos {len(df)} registros de {filename}")
            self.logger.info(f"Colunas: {df.columns.tolist()[:5]}...")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Erro ao extrair dados de {file_path}: {str(e)}")
            raise


class DataTransformer:
    """Transformador de dados para formato normalizado.
    
    Realiza a transformação dos dados extraídos para o formato
    normalizado esperado pelo banco de dados, incluindo:
    - Unpivot de colunas de meses
    - Padronização de nomes
    - Limpeza de valores
    - Validação de dados
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
        
        A estrutura dos dados é:
        - Coluna A (GRUPO_ECONOMICO): Contém o nome do grupo
        - Coluna B (VARIAVEL): Contém o nome da variável
        - Demais colunas: Contém os valores para cada mês (2018-01, 2018-02, etc.)
        
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
                self.logger.debug(f"Linha {idx}: Pulando linha vazia")
                continue
            
            # Limpa e padroniza o grupo
            grupo_limpo = self._clean_text(grupo)
            variavel_limpa = self._clean_text(variavel)
            
            # Mapeia variável para código
            variavel_codigo = ODSExtractor.VARIABLE_MAPPING.get(variavel_limpa, variavel_limpa)
            
            self.logger.debug(f"Linha {idx}: Grupo='{grupo_limpo}', Variável='{variavel_codigo}'")
            
            # Processa cada mês
            valores_processados = 0
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
                    valores_processados += 1
                    
                except Exception as e:
                    self.logger.debug(f"Erro ao processar célula na linha {idx}, coluna {month_col}: {str(e)}")
            
            if valores_processados > 0:
                self.logger.debug(f"Linha {idx}: {valores_processados} valores processados")
        
        self.logger.info(f"Transformados {len(records)} registros")
        return records
    
    def _is_month_column(self, col: str) -> bool:
        """Verifica se a coluna é um mês no formato YYYY-MM.
        
        Args:
            col: Nome da coluna
            
        Returns:
            bool: True se for coluna de mês
        """
        try:
            # Converte para string e remove espaços
            col_str = str(col).strip()
            
            # Verifica formato YYYY-MM
            if len(col_str) == 7 and col_str[4] == '-':
                year, month = col_str.split('-')
                return 2000 <= int(year) <= 2030 and 1 <= int(month) <= 12
            
            # Verifica se é um Timestamp pandas (comum em arquivos Excel)
            if hasattr(col, 'year') and hasattr(col, 'month'):
                return True
                
            return False
        except:
            return False
    
    def _clean_text(self, text: Any) -> str:
        """Limpa e padroniza texto.
        
        Args:
            text: Texto a ser limpo
            
        Returns:
            str: Texto limpo e padronizado
        """
        if pd.isna(text):
            return ""
        
        text = str(text).strip()
        
        # Remove espaços extras
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
        """Converte valor para float.
        
        Args:
            value: Valor a ser convertido
            
        Returns:
            Optional[float]: Valor convertido ou None se inválido
        """
        try:
            # Remove caracteres não numéricos comuns
            if isinstance(value, str):
                value = value.replace(',', '.')
                value = value.replace('%', '')
            
            return float(value)
        except:
            return None


class PostgreSQLLoader:
    """Carregador de dados no PostgreSQL.
    
    Realiza a carga dos dados transformados no banco de dados,
    incluindo:
    - Pool de conexões
    - Inserção em batch
    - Controle de transações
    - Tratamento de duplicatas
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
            
        Raises:
            psycopg2.Error: Em caso de erro no banco de dados
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
        """Insere um batch de registros.
        
        Args:
            cursor: Cursor do banco de dados
            batch: Lista de registros do batch
            
        Returns:
            int: Número de registros inseridos
        """
        # Prepara dados para inserção
        values = []
        for record in batch:
            values.append((
                record.ano_mes,
                record.grupo_economico,
                record.servico,
                record.variavel,
                record.valor,
                record.arquivo_origem,
                record.linha_origem,
                record.generate_hash()
            ))
        
        # Query de inserção com lookup das chaves
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
        
        # Executa inserções
        inserted = 0
        for value in values:
            params = {
                'ano_mes': value[0],
                'grupo_economico': value[1],
                'servico': value[2],
                'variavel': value[3],
                'valor': value[4],
                'arquivo_origem': value[5],
                'linha_origem': value[6],
                'hash_registro': value[7]
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

        return None

class ETLPipeline:
    """Orquestrador do pipeline ETL completo.
    
    Coordena a execução das etapas de extração, transformação e carga,
    garantindo a execução ordenada e tratamento de erros.
    """
    
    def __init__(self, config: Optional[ETLConfig] = None):
        """Inicializa o pipeline.
        
        Args:
            config: Configuração do ETL (usa padrão se não fornecida)
        """
        self.config = config or ETLConfig()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.extractor = ODSExtractor(self.config)
        self.transformer = DataTransformer(self.config)
        self.loader = PostgreSQLLoader(self.config)
    
    def run(self) -> Dict[str, Any]:
        """Executa o pipeline ETL completo.
        
        Processa todos os arquivos ODS encontrados no diretório de dados,
        realizando extração, transformação e carga para cada um.
        
        Returns:
            Dict[str, Any]: Estatísticas da execução
        """
        self.logger.info("Iniciando pipeline ETL IDA")
        start_time = datetime.now()
        
        stats = {
            'files_processed': 0,
            'records_extracted': 0,
            'records_transformed': 0,
            'records_loaded': 0,
            'errors': []
        }
        
        # Lista arquivos ODS no diretório
        data_path = Path(self.config.data_path)
        ods_files = list(data_path.glob("*.ods"))
        
        if not ods_files:
            self.logger.warning(f"Nenhum arquivo ODS encontrado em {data_path}")
            return stats
        
        # Processa cada arquivo com o loader
        with self.loader:
            for file_path in ods_files:
                try:
                    self.logger.info(f"Processando arquivo: {file_path}")
                    
                    # Extração
                    df = self.extractor.extract(str(file_path))
                    stats['records_extracted'] += len(df)
                    
                    # Transformação
                    records = self.transformer.transform(df)
                    stats['records_transformed'] += len(records)
                    
                    # Carga
                    loaded = self.loader.load(records)
                    stats['records_loaded'] += loaded
                    
                    stats['files_processed'] += 1
                    
                except Exception as e:
                    error_msg = f"Erro ao processar {file_path}: {str(e)}"
                    self.logger.error(error_msg)
                    stats['errors'].append(error_msg)
        
        # Finaliza e reporta estatísticas
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        self.logger.info(
            f"Pipeline ETL finalizado em {duration:.2f} segundos\n"
            f"Arquivos processados: {stats['files_processed']}\n"
            f"Registros extraídos: {stats['records_extracted']}\n"
            f"Registros transformados: {stats['records_transformed']}\n"
            f"Registros carregados: {stats['records_loaded']}\n"
            f"Erros: {len(stats['errors'])}"
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
        
        # Retorna código de saída baseado em erros
        if stats['errors']:
            sys.exit(1)
        else:
            sys.exit(0)
            
    except Exception as e:
        logging.error(f"Erro fatal no ETL: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
