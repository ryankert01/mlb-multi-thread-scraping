## Dependencies

download following dependencies to run the code.

```
selenium
beautifulsoup4
pandas
```

## Run

```
python main.py
```

for yearly representation required for this homework, we will have to aggregate years data in to one csv file adding a year column.

```
python aggregate.py
```

data desired will be at the main dir `./`

## Flaky

Might fail on certain task, but with proper error handling, we are able to parse it in one-run.

### Discovered Flaky

1. scrape more player then expected
2. sometimes when scraping expand items, `expand` is not pressed. resulting two same data.