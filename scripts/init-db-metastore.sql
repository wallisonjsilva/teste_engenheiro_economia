-- Cria o banco de dados para o Hive Metastore
CREATE DATABASE metastore;
-- Cria o usuário específico para o Hive
CREATE USER hive WITH PASSWORD 'hive';
-- Dá permissões totais ao usuário hive no banco metastore
GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;