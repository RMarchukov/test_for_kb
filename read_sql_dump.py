class ReadDump:
    def __init__(self, dump_file):
        self.dump_file = dump_file

    def read_dump(self, encoding=None):
        with open(self.dump_file, 'r', encoding=encoding) as file:
            sql_dump = file.read()
        return sql_dump

    def read_dump_for_params(self, encoding):
        with open(self.dump_file, 'r', encoding=encoding) as file:
            params = file.readlines()[0]
            p1 = params.index("(")
            p2 = params.index(")")
            p3 = params[p1 + 1: p2]
            p4 = p3.replace("`", "")
            p5 = p4.replace(",", "")
            l1 = p5.split()
        return l1
