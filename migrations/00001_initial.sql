-- +goose Up

create table merchants (
  id           text primary key,
  display_name text not null,
  category     text not null,
  created_at   timestamptz not null default now()
);
insert into merchants (id, display_name, category) values
  ('m-books',  'Beispiel Bücher',    'retail'),
  ('m-coffee', 'Kissaten Koffie',    'food_beverage'),
  ('m-cloud',  'Nimbus Cloud Corp',  'saas'),
  ('m-games',  'Pixel Arcade',       'entertainment');

-- Demo payment instruments (PCI stance: tokens only — no PAN/CVV anywhere).
-- Fixed ids shared by all demo users; a real system vaults per user.
create table instruments (
  id         uuid primary key,
  method     text not null check (method in ('card','wallet')),
  brand      text,
  last4      char(4),
  exp_month  int,
  exp_year   int,
  gateway_token text not null
);
insert into instruments (id, method, brand, last4, exp_month, exp_year, gateway_token) values
  ('00000000-0000-0000-0000-0000000ca4d1', 'card',   'visa', '4242', 12, 2030, 'tok_demo_visa_4242'),
  ('00000000-0000-0000-0000-0000000a11e7', 'wallet', null,   null,   null, null, 'tok_demo_wallet');

-- Append-only FX rates (single direction per pair; inverses derived).
create table exchange_rates (
  id         uuid primary key,
  base       char(3) not null,
  quote      char(3) not null,
  rate       numeric(18,8) not null check (rate > 0),
  source     text not null default 'seed',
  valid_from timestamptz not null default now()
);
create index exchange_rates_pair_idx on exchange_rates (base, quote, valid_from desc);
insert into exchange_rates (id, base, quote, rate) values
  (gen_random_uuid(), 'USD', 'EUR', 0.92000000),
  (gen_random_uuid(), 'USD', 'GBP', 0.79000000),
  (gen_random_uuid(), 'USD', 'JPY', 157.25000000);

-- Quotes lock a rate for a payment until expiry.
create table fx_quotes (
  id          uuid primary key,
  base        char(3) not null,
  quote       char(3) not null,
  rate        numeric(18,8) not null,
  rate_id     uuid not null,
  created_at  timestamptz not null default now(),
  expires_at  timestamptz not null
);

-- The saga aggregate. status = client-facing PaymentIntent lifecycle;
-- step = internal saga position (resume point after a crash).
create table payments (
  id             uuid primary key,
  user_id        uuid not null,
  account_id     uuid not null,
  merchant_id    text not null references merchants(id),
  instrument_id  uuid not null references instruments(id),
  amount         bigint not null check (amount > 0),
  currency       char(3) not null,
  fx_quote_id    uuid references fx_quotes(id),
  status         text not null default 'processing' check
                 (status in ('requires_action','processing','succeeded','failed','canceled','refunded')),
  step           text not null default 'requested' check
                 (step in ('requested','fraud_screened','funds_held','submitted_to_gateway',
                           'captured','recorded','notified','compensating','done')),
  hold_id        uuid,
  psp_reference  text,
  failure_code   text,
  failure_detail text,
  risk_score     int,
  version        bigint not null default 0,
  created_at     timestamptz not null default now(),
  updated_at     timestamptz not null default now()
);
create index payments_user_idx on payments (user_id, created_at desc);
create index payments_resume_idx on payments (updated_at) where status = 'processing';

-- Stripe-style idempotency records for POST /payments.
create table idempotency_keys (
  key           text not null,
  user_id       uuid not null,
  request_hash  char(64) not null,
  response_code int,
  response_body jsonb,
  locked_at     timestamptz,
  created_at    timestamptz not null default now(),
  primary key (user_id, key)
);

create table outbox (
  id            uuid primary key,
  aggregatetype text not null,
  aggregateid   text not null,
  type          text not null,
  payload       jsonb not null,
  created_at    timestamptz not null default now(),
  processed_at  timestamptz
);
create index outbox_unprocessed_idx on outbox (id) where processed_at is null;

create table processed_events (
  event_id     uuid primary key,
  processed_at timestamptz not null default now()
);

-- +goose Down
drop table if exists processed_events, outbox, idempotency_keys, payments,
  fx_quotes, exchange_rates, instruments, merchants cascade;
