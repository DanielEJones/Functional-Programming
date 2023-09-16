class Partial:
    def __init__(self, function, *args, **kwargs):
        self.num_args = function.__code__.co_argcount
        self.func = function
        self.args, self.kwargs = args, kwargs

    def __call__(self, *args, **kwargs):
        total_args, total_kwargs = self.args + args, dict(**self.kwargs, **kwargs)
        total_num = len(total_args) + len(total_kwargs)
        return self.func(*total_args, **total_kwargs) if total_num >= self.num_args else Partial(self.func, *total_args, **total_kwargs)


def curryable(func):
    return Partial(func)


@curryable
def select_entries(entries: list[int], data: list):
    return [entry for pos, entry in enumerate(data) if pos in entries] if len(entries) > 1 else data[entries[0]]


@curryable
def remove_entries(entries: list[int], data: list) -> list:
    return [entry for pos, entry in enumerate(data) if pos not in entries]


@curryable
def filter_by(key: callable, data: list) -> list:
    return [item for item in data if key(item)]


@curryable
def distinct(key: callable, data: list) -> list:
    return list({key(value): value for value in data}.values())


@curryable
def operating_on(entries: list[int], do: callable, data: list) -> list:
    return [do(entry) if pos in entries else entry for pos, entry in enumerate(data)]


@curryable
def do_each(funcs: list[callable], data: list) -> list:
    return [func(data) for func in funcs]


@curryable
def for_each(func: callable, data: list) -> list:
    return [func(item) for item in data]


@curryable
def reduce(func: callable, sequence: list, initial=None) -> any:
    return reduce(func, sequence[1:], func(sequence[0], initial)) if len(sequence) > 0 else initial


@curryable
def pipeline(funcs: list[callable], data: any) -> any:
    return reduce(lambda f, x: f(x), funcs, data)


def extract_file(file_path: str) -> list:
    with open(file_path, 'r') as file:
        return list(file)


def transpose(matrix: list[list]) -> list[list]:
    return list(map(list, zip(*matrix)))


def cast_to_type(value: str) -> any:
    for type_ in (int, float):
        try:
            return type_(value)
        except ValueError:
            pass
    return value


def mean(data: list) -> float:
    return sum(data) / len(data)


def sample_variance(data: list) -> float:
    return sum((val - mean(data)) ** 2 for val in data)


def variance_of(data: list) -> float:
    return sample_variance(data) / (len(data) - 1)


def correlation_between(a: list, b: list) -> float:
    return sum((ai - mean(a)) * (bi - mean(b)) for ai, bi in zip(a, b))/(sample_variance(a) * sample_variance(b)) ** 0.5


@curryable
def deep_round(places: int, data: list) -> list:
    return [round(item, places) if isinstance(item, float)
            else deep_round(places, item) if isinstance(item, list) else item for item in data]


@curryable
def main(csv_file: str, region: str) -> list:
    return pipeline([
        # Extract the file and clean the rows
        operating_on([0])(
            pipeline([
                extract_file,
                for_each(
                    pipeline([
                        lambda row: row.split(','),
                        for_each(
                            pipeline([
                                # Strip redundant characters, normalise the case, cast entries to an appropriate type
                                lambda entry: entry.strip('\n\"'),
                                lambda entry: entry.lower(),
                                cast_to_type
                            ])
                        )
                    ])
                ),
                distinct(lambda row: row[0])
            ])
        ),
        # Standardise the case of the target region
        operating_on([1])(lambda key: key.lower()),
        # Filter out irrelevant rows
        lambda data: filter_by(lambda row: row[5] == data[1])(data[0]),
        # Remove redundant columns
        transpose,
        remove_entries([5]),
        transpose,
        # Calculate final statistics
        do_each([
            # Find the countries with maximum and minimum populations
            pipeline([
                filter_by(lambda country: country[2] >= 0),
                do_each([
                    lambda countries: max(countries, key=lambda country: country[1])[0],
                    lambda countries: min(countries, key=lambda country: country[1])[0]
                ]),
            ]),
            # Average and Standard Deviation of the populations
            pipeline([
                transpose,
                select_entries([1]),
                do_each([
                    mean,
                    pipeline([
                        variance_of,
                        lambda x: x ** 0.5
                    ])
                ])
            ]),
            # Sorted Densities
            pipeline([
                transpose,
                select_entries([0, 1, 4]),
                transpose,
                for_each(
                    lambda row: [row[0], row[1] / row[2]]
                ),
                lambda data: sorted(data, key=lambda row: -row[1])
            ]),
            # Correlation between Land Area and Population
            pipeline([
                transpose,
                select_entries([1, 4]),
                lambda data: correlation_between(data[0], data[1])
            ])
        ]),
        deep_round(4)
    ])([csv_file, region])


if __name__ == '__main__':
    for_each(
        do_each([
            lambda country: print(country.upper()),
            pipeline([
                main('countries.csv'),
                for_each(print)
            ]),
            lambda x: print('')
        ])
    )(['asia', 'africa', 'Europe', 'Latin America & Caribbean', 'northern America', 'oceania'])
