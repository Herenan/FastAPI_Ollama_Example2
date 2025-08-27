import os
import snowflake.connector
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

def get_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        raise HTTPException(status_code=500, detail="Internal server error - could not connect to database")

app = FastAPI()

class Book(BaseModel):
    id: int
    title: str
    author: str
    year: int

class CreateBook(BaseModel):
    title: str
    author: str
    year: int

@app.get("/books", response_model=List[Book])
def get_all_books():
    conn = get_snowflake_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT ID, TITLE, AUTHOR, YEAR FROM BOOKS ORDER BY ID;")
            results = cur.fetchall()

            books = [Book(id=row[0], title=row[1], author=row[2], year=row[3]) for row in results]
            return books
    finally:
        conn.close()

@app.get("/books/{book_id}", response_model=Book)
def get_book_by_id(book_id: int):
    conn = get_snowflake_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT ID, TITLE, AUTHOR, YEAR FROM BOOKS WHERE ID = %s;", (book_id,))
            result = cur.fetchone()

            if result:
                book = Book(id=result[0], title=result[1], author=result[2], year=result[3])
                return book
            else:
                raise HTTPException(status_code=404, detail=f"Book with id {book_id} not found")
    finally:
        conn.close()

@app.post("/books", response_model=Book, status_code=201)
def create_book(new_book: CreateBook):
    conn = get_snowflake_connection()
    try:
        with conn.cursor() as cur:
            sql = "INSERT INTO BOOKS (TITLE, AUTHOR, YEAR) VALUES (%s, %s, %s);"
            cur.execute(sql, (new_book.title, new_book.author, new_book.year))

            cur.execute("SELECT ID, TITLE, AUTHOR, YEAR FROM BOOKS ORDER BY ID DESC LIMIT 1;")
            created_book_row = cur.fetchone()
            
            if created_book_row:
                 return Book(id=created_book_row[0], title=created_book_row[1], author=created_book_row[2], year=created_book_row[3])
            else:
                 raise HTTPException(status_code=500, detail="Failed to create and retrieve the book")
    finally:
        conn.close()

@app.put("/books/{book_id}", response_model=Book)
def update_book(book_id: int, updated_book_data: CreateBook):
    conn = get_snowflake_connection()
    try:
        with conn.cursor() as cur:
            sql = """
                UPDATE BOOKS
                SET TITLE = %s, AUTHOR = %s, YEAR = %s
                WHERE ID = %s;
            """
            cur.execute(sql, (updated_book_data.title, updated_book_data.author, updated_book_data.year, book_id))
            
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail=f"Book with id {book_id} not found")

            return get_book_by_id(book_id)
    finally:
        conn.close()

@app.delete("/books/{book_id}", status_code=204)
def delete_book(book_id: int):
    conn = get_snowflake_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM BOOKS WHERE ID = %s;", (book_id,))
            
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail=f"Book with id {book_id} not found")
            
            return
    finally:
        conn.close()
