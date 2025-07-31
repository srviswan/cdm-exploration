-- PostgreSQL initialization script for Equity Swaps Microservices
-- Creates databases and initial schema for position management, lot tracking, and cashflow generation

-- Create main database
CREATE DATABASE equity_swaps;
CREATE DATABASE cdm_events;

-- Create users and grant permissions
CREATE USER equity_user WITH PASSWORD 'equity_pass';
GRANT ALL PRIVILEGES ON DATABASE equity_swaps TO equity_user;
GRANT ALL PRIVILEGES ON DATABASE cdm_events TO equity_user;

-- Connect to equity_swaps database to create schema
\c equity_swaps;

-- Create schema for position management
CREATE SCHEMA IF NOT EXISTS positions;
CREATE SCHEMA IF NOT EXISTS lots;
CREATE SCHEMA IF NOT EXISTS cashflows;
CREATE SCHEMA IF NOT EXISTS instruments;
CREATE SCHEMA IF NOT EXISTS audit;

-- Grant schema permissions
GRANT ALL ON SCHEMA positions TO equity_user;
GRANT ALL ON SCHEMA lots TO equity_user;
GRANT ALL ON SCHEMA cashflows TO equity_user;
GRANT ALL ON SCHEMA instruments TO equity_user;
GRANT ALL ON SCHEMA audit TO equity_user;

-- Create positions table
CREATE TABLE positions.position (
    position_id VARCHAR(100) PRIMARY KEY,
    portfolio_id VARCHAR(100) NOT NULL,
    instrument_id VARCHAR(100) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    exchange VARCHAR(10) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    quantity DECIMAL(20,8) NOT NULL,
    average_cost DECIMAL(20,8) NOT NULL,
    current_price DECIMAL(20,8),
    market_value DECIMAL(20,2),
    unrealized_pnl DECIMAL(20,2),
    realized_pnl DECIMAL(20,2) DEFAULT 0,
    total_pnl DECIMAL(20,2),
    risk_metrics JSONB,
    status VARCHAR(20) DEFAULT 'OPEN',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    version BIGINT DEFAULT 1
);

-- Create lots table
CREATE TABLE lots.lot (
    lot_id VARCHAR(100) PRIMARY KEY,
    position_id VARCHAR(100) NOT NULL,
    trade_id VARCHAR(100) NOT NULL,
    instrument_id VARCHAR(100) NOT NULL,
    original_quantity DECIMAL(20,8) NOT NULL,
    remaining_quantity DECIMAL(20,8) NOT NULL,
    cost_basis DECIMAL(20,2) NOT NULL,
    unit_cost DECIMAL(20,8) NOT NULL,
    current_price DECIMAL(20,8),
    unrealized_pnl DECIMAL(20,2),
    realized_pnl DECIMAL(20,2) DEFAULT 0,
    accounting_method VARCHAR(20) NOT NULL,
    open_date TIMESTAMP WITH TIME ZONE NOT NULL,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    status VARCHAR(20) DEFAULT 'OPEN',
    tags JSONB,
    version BIGINT DEFAULT 1,
    
    FOREIGN KEY (position_id) REFERENCES positions.position(position_id)
);

-- Create cashflows table
CREATE TABLE cashflows.cashflow (
    cashflow_id VARCHAR(100) PRIMARY KEY,
    position_id VARCHAR(100) NOT NULL,
    portfolio_id VARCHAR(100) NOT NULL,
    instrument_id VARCHAR(100) NOT NULL,
    cashflow_type VARCHAR(30) NOT NULL,
    amount DECIMAL(20,2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    payment_date DATE NOT NULL,
    accrual_start_date DATE,
    accrual_end_date DATE,
    day_count_fraction DECIMAL(10,8),
    notional_amount DECIMAL(20,2),
    rate DECIMAL(10,6),
    spread DECIMAL(10,6),
    multiplier DECIMAL(10,4),
    adjustment_factor DECIMAL(10,6),
    discount_factor DECIMAL(10,6),
    present_value DECIMAL(20,2),
    status VARCHAR(20) DEFAULT 'PROJECTED',
    calculation_method VARCHAR(50),
    business_day_convention VARCHAR(20),
    calendar VARCHAR(10),
    reset_date DATE,
    fixing_date DATE,
    settlement_date DATE,
    payer_party VARCHAR(100),
    receiver_party VARCHAR(100),
    calculation_details JSONB,
    risk_factors JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    version BIGINT DEFAULT 1,
    
    FOREIGN KEY (position_id) REFERENCES positions.position(position_id)
);

-- Create instruments table
CREATE TABLE instruments.equity_swap_instrument (
    instrument_id VARCHAR(100) PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    isin VARCHAR(12),
    cusip VARCHAR(9),
    sedol VARCHAR(7),
    ric VARCHAR(50),
    bloomberg VARCHAR(50),
    exchange VARCHAR(10) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    instrument_type VARCHAR(30) NOT NULL,
    underlying_asset_id VARCHAR(100),
    underlying_symbol VARCHAR(20),
    underlying_name VARCHAR(200),
    asset_class VARCHAR(20),
    sector VARCHAR(50),
    industry VARCHAR(50),
    country VARCHAR(3),
    region VARCHAR(50),
    notional_amount DECIMAL(20,2),
    multiplier DECIMAL(10,4),
    maturity_date DATE,
    trade_date DATE,
    settlement_date DATE,
    reset_frequency VARCHAR(20),
    payment_frequency VARCHAR(20),
    day_count_convention VARCHAR(20),
    business_day_convention VARCHAR(20),
    market_cap DECIMAL(20,2),
    shares_outstanding DECIMAL(20,0),
    average_daily_volume DECIMAL(20,0),
    risk_factors JSONB,
    contract_specifications JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create audit tables
CREATE TABLE audit.position_audit (
    audit_id BIGSERIAL PRIMARY KEY,
    position_id VARCHAR(100) NOT NULL,
    operation VARCHAR(10) NOT NULL, -- INSERT, UPDATE, DELETE
    old_values JSONB,
    new_values JSONB,
    changed_by VARCHAR(100),
    changed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE audit.lot_audit (
    audit_id BIGSERIAL PRIMARY KEY,
    lot_id VARCHAR(100) NOT NULL,
    operation VARCHAR(10) NOT NULL,
    old_values JSONB,
    new_values JSONB,
    changed_by VARCHAR(100),
    changed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE audit.cashflow_audit (
    audit_id BIGSERIAL PRIMARY KEY,
    cashflow_id VARCHAR(100) NOT NULL,
    operation VARCHAR(10) NOT NULL,
    old_values JSONB,
    new_values JSONB,
    changed_by VARCHAR(100),
    changed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX idx_positions_portfolio_id ON positions.position(portfolio_id);
CREATE INDEX idx_positions_symbol ON positions.position(symbol);
CREATE INDEX idx_positions_status ON positions.position(status);
CREATE INDEX idx_positions_updated_at ON positions.position(updated_at);

CREATE INDEX idx_lots_position_id ON lots.lot(position_id);
CREATE INDEX idx_lots_trade_id ON lots.lot(trade_id);
CREATE INDEX idx_lots_status ON lots.lot(status);
CREATE INDEX idx_lots_accounting_method ON lots.lot(accounting_method);
CREATE INDEX idx_lots_open_date ON lots.lot(open_date);

CREATE INDEX idx_cashflows_position_id ON cashflows.cashflow(position_id);
CREATE INDEX idx_cashflows_portfolio_id ON cashflows.cashflow(portfolio_id);
CREATE INDEX idx_cashflows_payment_date ON cashflows.cashflow(payment_date);
CREATE INDEX idx_cashflows_status ON cashflows.cashflow(status);
CREATE INDEX idx_cashflows_type ON cashflows.cashflow(cashflow_type);

CREATE INDEX idx_instruments_symbol ON instruments.equity_swap_instrument(symbol);
CREATE INDEX idx_instruments_isin ON instruments.equity_swap_instrument(isin);
CREATE INDEX idx_instruments_exchange ON instruments.equity_swap_instrument(exchange);
CREATE INDEX idx_instruments_type ON instruments.equity_swap_instrument(instrument_type);

-- Create audit triggers
CREATE OR REPLACE FUNCTION audit.position_audit_trigger()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO audit.position_audit(position_id, operation, old_values)
        VALUES (OLD.position_id, 'DELETE', to_jsonb(OLD));
        RETURN OLD;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO audit.position_audit(position_id, operation, old_values, new_values)
        VALUES (NEW.position_id, 'UPDATE', to_jsonb(OLD), to_jsonb(NEW));
        RETURN NEW;
    ELSIF TG_OP = 'INSERT' THEN
        INSERT INTO audit.position_audit(position_id, operation, new_values)
        VALUES (NEW.position_id, 'INSERT', to_jsonb(NEW));
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER position_audit_trigger
    AFTER INSERT OR UPDATE OR DELETE ON positions.position
    FOR EACH ROW EXECUTE FUNCTION audit.position_audit_trigger();

-- Create similar triggers for lots and cashflows
CREATE OR REPLACE FUNCTION audit.lot_audit_trigger()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO audit.lot_audit(lot_id, operation, old_values)
        VALUES (OLD.lot_id, 'DELETE', to_jsonb(OLD));
        RETURN OLD;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO audit.lot_audit(lot_id, operation, old_values, new_values)
        VALUES (NEW.lot_id, 'UPDATE', to_jsonb(OLD), to_jsonb(NEW));
        RETURN NEW;
    ELSIF TG_OP = 'INSERT' THEN
        INSERT INTO audit.lot_audit(lot_id, operation, new_values)
        VALUES (NEW.lot_id, 'INSERT', to_jsonb(NEW));
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER lot_audit_trigger
    AFTER INSERT OR UPDATE OR DELETE ON lots.lot
    FOR EACH ROW EXECUTE FUNCTION audit.lot_audit_trigger();

CREATE OR REPLACE FUNCTION audit.cashflow_audit_trigger()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO audit.cashflow_audit(cashflow_id, operation, old_values)
        VALUES (OLD.cashflow_id, 'DELETE', to_jsonb(OLD));
        RETURN OLD;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO audit.cashflow_audit(cashflow_id, operation, old_values, new_values)
        VALUES (NEW.cashflow_id, 'UPDATE', to_jsonb(OLD), to_jsonb(NEW));
        RETURN NEW;
    ELSIF TG_OP = 'INSERT' THEN
        INSERT INTO audit.cashflow_audit(cashflow_id, operation, new_values)
        VALUES (NEW.cashflow_id, 'INSERT', to_jsonb(NEW));
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER cashflow_audit_trigger
    AFTER INSERT OR UPDATE OR DELETE ON cashflows.cashflow
    FOR EACH ROW EXECUTE FUNCTION audit.cashflow_audit_trigger();

-- Grant permissions on all tables
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA positions TO equity_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA lots TO equity_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA cashflows TO equity_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA instruments TO equity_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA audit TO equity_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA audit TO equity_user;

-- Create CDM events database schema
\c cdm_events;

CREATE SCHEMA IF NOT EXISTS events;
GRANT ALL ON SCHEMA events TO equity_user;

-- Create events table for CDM event sourcing
CREATE TABLE events.business_event (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_id VARCHAR(100) NOT NULL,
    aggregate_type VARCHAR(50) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    event_data JSONB NOT NULL,
    metadata JSONB,
    sequence_number BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by VARCHAR(100)
);

CREATE INDEX idx_events_aggregate_id ON events.business_event(aggregate_id);
CREATE INDEX idx_events_aggregate_type ON events.business_event(aggregate_type);
CREATE INDEX idx_events_event_type ON events.business_event(event_type);
CREATE INDEX idx_events_created_at ON events.business_event(created_at);
CREATE INDEX idx_events_sequence ON events.business_event(sequence_number);

-- Create snapshot table
CREATE TABLE events.aggregate_snapshot (
    aggregate_id VARCHAR(100) PRIMARY KEY,
    aggregate_type VARCHAR(50) NOT NULL,
    snapshot_data JSONB NOT NULL,
    sequence_number BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA events TO equity_user;

-- Insert sample data for testing
\c equity_swaps;

-- Sample instrument
INSERT INTO instruments.equity_swap_instrument (
    instrument_id, symbol, isin, exchange, currency, instrument_type,
    underlying_symbol, underlying_name, asset_class, notional_amount,
    multiplier, trade_date, maturity_date
) VALUES (
    'AAPL-EQUITY-SWAP-001', 'AAPL', 'US0378331005', 'NASDAQ', 'USD', 'EQUITY_SWAP',
    'AAPL', 'Apple Inc.', 'EQUITY', 1000000.00,
    1.0, CURRENT_DATE, CURRENT_DATE + INTERVAL '1 year'
);

-- Sample position
INSERT INTO positions.position (
    position_id, portfolio_id, instrument_id, symbol, exchange, currency,
    quantity, average_cost, current_price, market_value,
    unrealized_pnl, total_pnl
) VALUES (
    'POS-AAPL-001', 'PORTFOLIO-ALPHA', 'AAPL-EQUITY-SWAP-001', 'AAPL', 'NASDAQ', 'USD',
    1000.00, 150.50, 155.25, 155250.00,
    4750.00, 4750.00
);

COMMIT;