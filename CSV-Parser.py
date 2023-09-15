def extract_lines(csv_file: str):
    with open(csv_file, 'r') as file:
        return list(file)


def transpose(matrix: list[list]):
    return list(map(list, zip(*matrix)))


def distinct(key=lambda x: x):
    def filtered(data):
        return list({key(item): item for item in data}.values())
    return filtered


def reduce(function, sequence, initial=None):
    return reduce(function, sequence[1:], function(sequence[0], initial)) if len(sequence) > 0 else initial


def for_each(func):
    def process(data):
        return list(map(func, data))
    return process


def pipeline(*funcs: callable):
    def execute(data: any) -> any:
        return reduce(lambda f, x: f(x), funcs, data)
    return execute


def remove_entries(*x: int):
    def do_removal(data: list) -> list:
        return [item for pos, item in enumerate(data) if pos not in x]
    return do_removal


def select_entries(*x: int):
    def do_selection(data: list) -> list:
        return [item for pos, item in enumerate(data) if pos in x] if len(x) > 1 else data[x[0]]
    return do_selection


def filter_by(key=lambda x: x):
    def do_filter(data: list) -> list:
        return [item for item in data if key(item)]
    return do_filter


def calculate(*calculations):
    def do_calculations(data: list) -> list:
        return [calculation(data) for calculation in calculations]
    return do_calculations


def mean(data: list) -> float:
    return sum(data) / len(data)


def main(csv_file: str, region: str) -> list:
    return pipeline(
        extract_lines,
        # Split the lines into entries stored in a list, and standardise the entries
        for_each(
            pipeline(
                lambda x: x.split(','),
                for_each(
                    pipeline(
                        lambda x: x.strip('\n\"'),
                        lambda x: x.lower()
                    )
                )
            )
        ),
        # Remove the headers
        remove_entries(0),
        # Select the rows and columns we need
        distinct(key=lambda x: x[0]),
        filter_by(key=lambda x: x[5] == region.lower()),
        # Removes the now-redundant region column
        transpose,
        remove_entries(5),
        transpose,
        # Calculate the final statistics
        calculate(
            # Countries with maximum and minimum populations
            pipeline(
                filter_by(key=lambda country: float(country[2]) >= 0),
                calculate(
                    lambda rows: max(rows, key=lambda row: int(row[1]))[0],
                    lambda rows: min(rows, key=lambda row: int(row[1]))[0]
                )
            ),
            # Average Population and it's Standard Deviation
            pipeline(
                transpose,
                select_entries(1),
                for_each(int),
                calculate(
                    pipeline(
                        mean,
                        lambda val: round(val, 4)
                    ),
                    pipeline(
                        calculate(
                            # Gets the average and retains the population in its own list
                            mean,
                            lambda row: list(row)
                        ),
                        # Calculates the Standard Deviation
                        lambda x: sum((x[0] - val) ** 2 for val in x[1]) / (len(x[1]) - 1),
                        lambda x: x ** 0.5,
                        lambda val: round(val, 4)
                    )
                )
            ),
            # Sorted Densities
            pipeline(
                for_each(
                    pipeline(
                        lambda row: [row[0], int(row[1]) / int(row[4])],
                        lambda row: [row[0], round(row[1], 4)]
                    )
                ),
                lambda data: sorted(data, key=lambda row: -row[1])
            ),
            # Correlation between Population and Area
            pipeline(
                transpose,
                select_entries(1, 4),
                calculate(
                    for_each(
                        pipeline(
                            for_each(int),
                            calculate(list, mean),
                            lambda data: sum((x - data[1]) ** 2 for x in data[0])
                        )
                    ),
                    pipeline(
                        for_each(for_each(int)),
                        calculate(for_each(mean), list),
                        lambda data: sum((x - data[0][0]) * (y - data[0][1]) for x, y in zip(data[1][0], data[1][1]))
                    )
                ),
                lambda data: data[1] / (data[0][0] * data[0][1]) ** 0.5,
                lambda data: round(data, 4)
            )
        )
    )(csv_file)


if __name__ == '__main__':
    for_each(print)(main('./countries.csv', 'Asia'))
