-- Metadata database for telescreen itself.
CREATE DATABASE telescreen;

-- Demo source database to capture changes from.
CREATE DATABASE shop;

\connect shop

CREATE TABLE customers (
    id          bigserial PRIMARY KEY,
    email       text        NOT NULL UNIQUE,
    full_name   text        NOT NULL,
    phone       text,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    id           bigserial PRIMARY KEY,
    customer_id  bigint        NOT NULL REFERENCES customers (id),
    status       text          NOT NULL DEFAULT 'new',
    total        numeric(12,2) NOT NULL,
    meta         jsonb         NOT NULL DEFAULT '{}',
    created_at   timestamptz   NOT NULL DEFAULT now()
);

-- Full replica identity makes the old row image available for updates and deletes.
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;

INSERT INTO customers (email, full_name, phone) VALUES
    ('winston@oceania.gov', 'Winston Smith', '+7 900 000-19-84'),
    ('julia@oceania.gov', 'Julia', NULL);

INSERT INTO orders (customer_id, status, total, meta) VALUES
    (1, 'paid', 19.84, '{"items": ["diary", "pen"]}'),
    (2, 'new', 101.00, '{"items": ["coffee"]}');
