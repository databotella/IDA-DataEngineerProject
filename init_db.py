#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
init_db — Inicializa e aplica o schema do banco idadatamart.

Description:
    Fecha conexões ativas em idadatamart, dropa o database, recria-o e
    aplica todas as definições e dados do schema em 'schema_star.sql'.

    O script lê o arquivo SQL, divide os comandos por ponto-e-vírgula (';'),
    descarta comentários (linhas iniciadas com '--') e metacomandos psql
    (linhas iniciadas com '\\'), e executa cada comando em sequência.
    Para cada SQL executado, uma mensagem é impressa; em caso de erro,
    o comando exato que falhou e o erro são destacados.

Example:
    $ python init_db.py

Raises:
    SystemExit: Se houver erro na conexão ou na execução dos comandos.
"""

import sys
from urllib.parse import urlparse, urlunparse
import psycopg2
from psycopg2 import sql
from etl_ida import ETLConfig

def main():
    """
    Executa o processo de criação do database e aplicação do schema a partir de 'schema_star.sql'.

    Fluxo:
        1. Conecta ao banco 'postgres' e encerra conexões ativas no 'idadatamart'.
        2. Dropa e recria o database 'idadatamart'.
        3. Reconnecta ao 'idadatamart' via psycopg2.
        4. Abre 'schema_star.sql', parseia o conteúdo:
           - Divide por ';' para separar comandos.
           - Remove linhas vazias, comentários ('--') e metacomandos psql ('\\').
        5. Executa cada comando em sequência, imprimindo na saída:
           - o SQL executado.
           - em caso de falha, exibe 'SQL FALHOU:' seguido do comando e do erro.
    Args:
        None

    Returns:
        None

    Raises:
        SystemExit: Se houver erro na conexão ou na execução dos comandos.
    """
    config = ETLConfig()
    # Parsear DSN original
    parsed = urlparse(config.db_connection_string)
    # Conectar ao banco 'postgres' para criar o database
    default_netloc = parsed.netloc
    postgres_path = '/postgres'
    dsn_postgres = urlunparse(parsed._replace(path=postgres_path))
    try:
        conn = psycopg2.connect(dsn_postgres)
        conn.autocommit = True
        cur = conn.cursor()
        print("Encerrando conexões ativas em idadatamart...")
        cur.execute("""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = 'idadatamart'
              AND pid <> pg_backend_pid();
        """)
        print("Dropando database idadatamart...")
        cur.execute("DROP DATABASE IF EXISTS idadatamart;")
        print("Criando database idadatamart...")
        cur.execute("CREATE DATABASE idadatamart;")
        print("Database recriado.")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Erro criando/recriando database: {e}")
        sys.exit(1)

    # Conectar ao idadatamart
    try:
        conn2 = psycopg2.connect(config.db_connection_string)
        conn2.autocommit = True
        cur2 = conn2.cursor()
    except Exception as e:
        print(f"Erro conectando ao idadatamart: {e}")
        sys.exit(1)

    # Ler e executar comandos do schema script
    print("Executando schema_star.sql...")
    with open("schema_star.sql", "r", encoding="utf-8") as f:
        sql_text = f.read()
    # Remover linhas de conexão psql e comentários
    commands = []
    for part in sql_text.split(";"):
        cmd = part.strip()
        if not cmd or cmd.startswith("--") or cmd.startswith("\\"):
            continue
        commands.append(cmd)

    # Executar cada comando
    for cmd in commands:
        try:
            cur2.execute(cmd)
        except Exception as e:
            print(f"SQL FALHOU: {cmd}\nErro: {e}")
    cur2.close()
    conn2.close()
    print("Inicialização concluída com sucesso.")

if __name__ == "__main__":
    main()
