class Engine:
    def create_table_sql(self):
        raise NotImplementedError()


class MergeTree(Engine):
    def __init__(
        self,
        date_col,
        key_cols,
        sampling_expr=None,
        index_granularity=8192,
        replica_table_path=None,
        replica_name=None,
    ):
        self.date_col = date_col
        self.key_cols = key_cols
        self.sampling_expr = sampling_expr
        self.index_granularity = index_granularity
        self.replica_table_path = replica_table_path
        self.replica_name = replica_name
        if bool(replica_table_path) != bool(replica_name):
            raise ValueError('replica_table_path and replica_name must be supplied together')

    def create_table_sql(self):
        name = self.__class__.__name__
        if self.replica_name:
            name = 'Replicated' + name
        params = self._build_sql_params()
        clauses = [
            '%s(%s)' % (name, ', '.join(params)),
            'PARTITION BY toYYYYMM(%s)' % self.date_col,
            'ORDER BY (%s)' % ', '.join(self.key_cols),
        ]
        if self.sampling_expr:
            clauses.append('SAMPLE BY %s' % self.sampling_expr)
        clauses.append('SETTINGS index_granularity = %s' % self.index_granularity)
        return '\n'.join(clauses)

    def _build_sql_params(self):
        params = []
        if self.replica_name:
            params += ["'%s'" % self.replica_table_path, "'%s'" % self.replica_name]
        return params


class CollapsingMergeTree(MergeTree):
    def __init__(
        self,
        date_col,
        key_cols,
        sign_col,
        sampling_expr=None,
        index_granularity=8192,
        replica_table_path=None,
        replica_name=None,
    ):
        super().__init__(
            date_col, key_cols, sampling_expr, index_granularity, replica_table_path, replica_name
        )
        self.sign_col = sign_col

    def _build_sql_params(self):
        params = super()._build_sql_params()
        params.append(self.sign_col)
        return params


class SummingMergeTree(MergeTree):
    def __init__(
        self,
        date_col,
        key_cols,
        summing_cols=None,
        sampling_expr=None,
        index_granularity=8192,
        replica_table_path=None,
        replica_name=None,
    ):
        super().__init__(
            date_col, key_cols, sampling_expr, index_granularity, replica_table_path, replica_name
        )
        self.summing_cols = summing_cols

    def _build_sql_params(self):
        params = super()._build_sql_params()
        if self.summing_cols:
            params.append('(%s)' % ', '.join(self.summing_cols))
        return params


class ReplacingMergeTree(MergeTree):
    def __init__(
        self,
        date_col,
        key_cols,
        version_col=None,
        sampling_expr=None,
        index_granularity=8192,
        replica_table_path=None,
        replica_name=None,
    ):
        super().__init__(
            date_col,
            key_cols,
            sampling_expr,
            index_granularity,
            replica_table_path,
            replica_name,
        )
        self.version_col = version_col

    def _build_sql_params(self):
        params = super()._build_sql_params()
        if self.version_col:
            params.append(self.version_col)
        return params
