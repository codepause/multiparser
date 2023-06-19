## Multithreading parser with limits control rate

```
MultiThreadDriver spawns N instances of driver
Each driver takes data from common pool of requests (request handler)
request handler controls request rate and data distribution
```

Installation:
```
pip install -e .
```
[Demo code](https://github.com/codepause/multiparser/blob/master/multiparser/src/tests/main.py)

Usage:
```commandline
# create driver constructor:
client = Spot(api_key, api_secret)
builder = partial(BinanceDriver, client)

# Init app, modify speedometer rate if needed:
a = App(builder, n_workers=2, rps=10)

# init server answer parser:
getter = BinanceHistoryGetter()

# local queue init and fill:
dq = collections.deque()

time_start, time_end = convert_time(args)
times = split_time(time_start, time_end, args.granularity, args.limit)

for (local_time_start, local_time_end) in times:
    local_time_start = round(local_time_start.timestamp() * 1000)
    local_time_end = round(local_time_end.timestamp() * 1000)
    dq.append(Request(getter, args=(args.ticker, args.granularity),
                      kwargs={'limit': args.limit, 'startTime': local_time_start, 'endTime': local_time_end}))

# parse and save:
a.start()
a.start_parsing(dq, args.out_dir, verbose=True)
a.stop()
```

