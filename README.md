SQL Statement
-CREATE TABLE :
CREATE OR REPLACE TABLE BOOKS (
    ID INT AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
    TITLE VARCHAR(255),
    AUTHOR VARCHAR(255),
    YEAR INT
);

-INSERT DATA :
INSERT INTO BOOKS (TITLE, AUTHOR, YEAR) VALUES
('The Lord of the Rings', 'J.R.R. Tolkien', 1954),
('Dune', 'Frank Herbert', 1965),
('A Light in the Attic', 'J.R.R. Tolkien', 1929),
('Tipping the Velvet', 'J.R.R. Tolkien', 1915),
('Soumission', 'J.R.R. Tolkien', 1950),
('Sharp Objects', 'J.R.R. Tolkien', 1933),
('Sapiens: A Brief History of Humankind', 'J.R.R. Tolkien', 1943),
('The Requiem Red', 'J.R.R. Tolkien', 1923),
('The Dirty Little Secrets of Getting Your Dream Job', 'J.R.R. Tolkien', 1992),
('The Coming Woman: A Novel Based on the Life of the Infamous Feminist, Victoria Woodhull', 'J.R.R. Tolkien', 1935),
('The Boys in the Boat: Nine Americans and Their Epic Quest for Gold at the 1936 Berlin Olympics', 'J.R.R. Tolkien', 1936);
