-- +goose Up
-- The saga gains a checkpoint between "the processor took the money" and "our
-- ledger says so". Without a durable marker for that gap, a resumed worker
-- cannot tell whether the PSP already captured — and a sweeper that guesses
-- wrong would release a hold for money the card network has already collected.
alter table payments drop constraint if exists payments_step_check;
alter table payments add constraint payments_step_check
  check (step in ('requested','fraud_screened','funds_held','submitted_to_gateway',
                  'psp_captured','captured','recorded','notified','compensating','done'));

-- +goose Down
alter table payments drop constraint if exists payments_step_check;
alter table payments add constraint payments_step_check
  check (step in ('requested','fraud_screened','funds_held','submitted_to_gateway',
                  'captured','recorded','notified','compensating','done'));
