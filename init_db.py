#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de inicialização do banco de dados `idadatamart` e schema `ida`
com base no arquivo schema_star.sql.
"""

import sys
from urllib.parse import urlparse, urlunparse
import psycopg2
from psycopg2 import sql
from etl.etl_ida import ETLConfig

def main():
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
            print(f"Erro ao executar comando:\n{cmd}\n{e}")
    cur2.close()
    conn2.close()
    print("Inicialização concluída com sucesso.")

if __name__ == "__main__":
    main()
