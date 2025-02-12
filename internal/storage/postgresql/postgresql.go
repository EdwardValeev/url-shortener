package postgresql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"url-shortener/internal/storage"
)

type Storage struct {
	db *pgxpool.Pool
}

func New(connStr string) (*Storage, error) {
	const op = "storage.postgresql.New"

	db, err := pgxpool.New(context.Background(), connStr)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	err = db.Ping(context.Background())
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	_, err = db.Exec(context.Background(), `
	CREATE TABLE IF NOT EXISTS url(
	    id SERIAL PRIMARY KEY,
	    alias TEXT NOT NULL UNIQUE,
	    url TEXT NOT NULL);
	CREATE INDEX IF NOT EXISTS idx_alias ON url(alias);
	`)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &Storage{db: db}, nil
}

func (s *Storage) SaveURL(ctx context.Context, urlToSave, alias string) (int64, error) {
	const op = "storage.postgresql.SaveURL"
	var id int64

	err := s.db.QueryRow(ctx, `
		INSERT INTO url(url, alias) VALUES($1, $2) RETURNING id;
	`, urlToSave, alias).Scan(&id)
	if err != nil {
		// Обработка ошибки уникальности (если alias уже существует)
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "23505" { // Код 23505 = violation of unique constraint
			return 0, fmt.Errorf("%s: %w", op, storage.ErrURLExists)
		}

		// Обработка других ошибок
		return 0, fmt.Errorf("%s: %w", op, err)
	}

	return id, nil
}

func (s *Storage) GetURL(ctx context.Context, alias string) (string, error) {
	const op = "storage.postgresql.GetURL"
	var url string

	err := s.db.QueryRow(ctx, `SELECT url FROM url WHERE alias = $1;`, alias).Scan(&url)
	if err != nil {
		// Обработка ошибки отсутствия строки по ключу (запись не найдена)
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("%s: %w", op, storage.ErrURLNotFound)
		}
		// Обработка других ошибок
		return "", fmt.Errorf("%s: %w", op, err)
	}

	return url, nil
}

func (s *Storage) DeleteURL(ctx context.Context, alias string) error {
	const op = "storage.postgresql.DeleteURL"

	result, err := s.db.Exec(ctx, `DELETE FROM url WHERE alias = $1`, alias)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	// Проверяем, сколько строк было удалено
	rowsAffected := result.RowsAffected()
	if rowsAffected == 0 {
		// Если ни одна строка не была удалена, возвращаем ошибку
		return fmt.Errorf("%s: %w", op, storage.ErrURLNotFound)
	}

	return nil
}
