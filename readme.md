## Dependencies

download following dependencies to run the code.

```
pip install -r requirements.txt
```

## Run single-threadedly

```
python mlb.py
```

## Run with Multi-thread

```
python mlb-ray.py
```

for yearly representation required for this homework, we will have to aggregate years data in to one csv file adding a year column.

```
python validate.py
```

data desired will be at `./result`.

## Flaky

Might fail on certain task, but with proper error handling, we are able to parse it in one-run.

### Discovered Flaky

1. scrape more player then expected
2. sometimes when scraping expand items, `expand` is not pressed. resulting two same data.