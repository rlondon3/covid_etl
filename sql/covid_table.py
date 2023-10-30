CREATE_COVID_DATA_TABLE = 'CREATE TABLE IF NOT EXISTS covid_data (id SERIAL PRIMARY KEY, date DATE, recovered INT, confirmed_cases BIGINT, deaths BIGINT, country VARCHAR(50));'
TABLE_EXISTS = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'covid_data');"
UPLOAD = "COPY covid_data FROM STDIN WITH (FORMAT csv, HEADER, DELIMITER ',');"
GET_COVID_TABLE = 'SELECT * FROM covid_data;'
GET_COVID_DATA_BY_ID = "SELECT row_to_json(t) FROM covid_data t WHERE id = %s;"
DROP_TABLE = 'DROP TABLE covid_Data;'