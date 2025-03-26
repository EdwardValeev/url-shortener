package postgresql

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
	"url-shortener/internal/storage"
)

type Storage struct {
	db *pgxpool.Pool
}

func New(connStr string) (*Storage, error) {
	const op = "storage.postgresql.New"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	// Проверяем соединение с БД
	if err := db.Ping(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = db.Exec(ctx, `
	CREATE TABLE IF NOT EXISTS url(
	    id SERIAL PRIMARY KEY,
	    alias TEXT NOT NULL UNIQUE,
	    url TEXT NOT NULL);
	CREATE INDEX IF NOT EXISTS idx_alias ON url(alias);
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &Storage{db: db}, nil
}

func (s *Storage) SaveURL(urlToSave, alias string) (int64, error) {
	const op = "storage.postgresql.SaveURL"
	var id int64

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := s.db.QueryRow(ctx, `
		INSERT INTO url(url, alias) 
		VALUES($1, $2) 
		RETURNING id;
	`, urlToSave, alias).Scan(&id)
	if err != nil {
		// Обработка ошибки уникальности (если alias уже существует)
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
			return 0, fmt.Errorf("%s: %w", op, storage.ErrURLExists)
		}

		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("%s: no rows returned: %w", op, err)
		}

		// Обработка других ошибок
		return 0, fmt.Errorf("%s: %w", op, err)
	}

	return id, nil
}

func (s *Storage) GetURL(alias string) (string, error) {
	const op = "storage.postgresql.GetURL"
	var url string

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := s.db.QueryRow(ctx, `SELECT url FROM url WHERE alias = $1;`, alias).Scan(&url)
	if err != nil {
		// Обработка ошибки отсутствия строки по ключу (запись не найдена)
		if errors.Is(err, pgx.ErrNoRows) {
			return "", fmt.Errorf("%s: %w", op, storage.ErrURLNotFound)
		}
		// Обработка других ошибок
		return "", fmt.Errorf("%s: %w", op, err)
	}

	return url, nil
}

func (s *Storage) DeleteURL(alias string) error {
	const op = "storage.postgresql.DeleteURL"

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	result, err := s.db.Exec(ctx, `DELETE FROM url WHERE alias = $1`, alias)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	// Проверяем, сколько строк было удалено
	if rowsAffected := result.RowsAffected(); rowsAffected == 0 {
		// Если ни одна строка не была удалена, возвращаем ошибку
		return fmt.Errorf("%s: %w", op, storage.ErrURLNotFound)
	}

	return nil
}
