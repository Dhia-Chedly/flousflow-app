# Dataset

Local input: `raw/PS_20174392719_1491204439457_log.csv` from
[PaySim on Kaggle](https://www.kaggle.com/datasets/ealaxi/paysim1).
The full dataset is excluded from Git; place your copy in `data/raw/`.

Profile of the local file:

- 6,362,620 records and 11 columns, with no blank fields.
- Steps 1–743, ordered by step.
- 8,213 fraud-labelled transactions; 16 flagged transactions.
- All monetary values are nonnegative with at most two decimal places; some
  source strings use scientific notation.
- 16 zero-amount transactions, all fraud-labelled.

SHA-256:

```text
16910f90577b0d981bf8ff289714510bb89bc71bff7d3f220f024e287e4eea6b
```

Preserve the source bytes and record order: both contribute to event identity.
Balance equations do not always reconcile, so discrepancies are not rejected.

`sample/transactions.csv` contains three hand-authored synthetic records for local
development and tests. It is not an extract of the full dataset.
