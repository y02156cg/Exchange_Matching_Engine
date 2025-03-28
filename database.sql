CREATE TABLE accounts(
    account_id VARCHAR PRIMARY KEY,
    balance DECIMAL NOT NULL CHECK (balance >= 0)
);

CREATE TABLE symbols (
    symbol VARCHAR PRIMARY KEY
);

CREATE TABLE positions (
    account_id VARCHAR REFERENCES accounts(account_id),
    symbol VARCHAR REFERENCES symbos(symbol),
    amount DECIMAL NOT NULL CHECK (amount >= 0),
    PRIMARY KEY (account_id, symbol)
);

CREATE TABLE orders(
    order_id SERIAL PRIMARY KEY,
    account_id VARCHAR REFERENCES accounts(account_id),
    symbol VARCHAR REFERENCES symbols(symbol),
    amount DECIMAL NOT NULL,
    limit_price DECIMAL NOT NULL,
    remaining_amount DECIMAL NOT NULL,
    time_created TIMESTAMP NOT NULL DEFAULT NOW(),
    status VARCHAR NOT NULL CHECK (status IN ('open', 'executed', 'cancelled'))  
);

CREATE TABLE executions (
    execution_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    shares DECIMAL NOT NULL,
    price DECIMAL NOT NULL,
    time_executed TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_orders_symbol_status ON orders(symbol, status);
CREATE INDEX idx_orders_account_id ON orders(account_id);