\set ON_ERROR_STOP on
BEGIN;
CREATE SCHEMA IF NOT EXISTS shopvibe;

-- Checkout orders are committed here; Debezium publishes the committed changes.
CREATE TABLE IF NOT EXISTS shopvibe.customers (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS shopvibe.products (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    price NUMERIC(12,2) NOT NULL CHECK (price >= 0),
    stock INTEGER NOT NULL DEFAULT 0 CHECK (stock >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS shopvibe.orders (
    id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL REFERENCES shopvibe.customers(id),
    items JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(items) = 'array'),
    total_amount NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0),
    payment_method TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'APPROVED',
    ip_address TEXT,
    device_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMIT;
