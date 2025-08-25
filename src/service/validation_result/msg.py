import polars as pl


class ValidationMessages(object):
    """
    Store validation message result and analysis function
    """
    def __init__(
            self, id: str, 
            allowed_status: list = ['review', 'error'],
            allowed_ignored_status: list = ['review', 'force']
        ):
        self._id = id

        # Polars dataframe and schema
        self._df_schema = {
            'msg': pl.String,
            'status': pl.String,
            'status_idx': pl.Int16,
            'ignore_in': pl.String,
            'id': pl.String
        }

        self._df = pl.DataFrame([], schema=self._df_schema)
        self._messages = [self._df]

        self._supported_status = allowed_status
        self._supported_ignore = allowed_ignored_status

    def add_message(self, msg: str, msg_status: str, ignore_in=None):
        """
        Add message to array.
        """
        if msg_status not in self._supported_status:
            raise ValueError(f"message status {msg_status} is not supported.")
        
        if (ignore_in not in self._supported_ignore) and (ignore_in is not None):
            raise ValueError(f"ignore message in {ignore_in} is not supported.")
        
        new_row = pl.DataFrame({
            'msg': [msg],  
            'status': [msg_status], 
            'status_idx': [self._supported_status.index(msg_status)],
            'ignore_in': [ignore_in], 
            'id': [self._id]
        }, schema=self._df_schema)

        self._messages.append(new_row)
        # self._df = pl.concat([self._df, new_row])

        return self
    
    def add_messages(self, df: pl.DataFrame, msg_status:str, ignore_in: str = None):
        """
        Concat a dataframe with the same schema.
        """
        if msg_status not in self._supported_status:
            raise ValueError(f"message status {msg_status} is not supported.")
        
        if (ignore_in not in self._supported_ignore) and (ignore_in is not None):
            raise ValueError(f"ignore message in {ignore_in} is not supported.")
        
        self._df = self._messages.append(
            df.with_columns(
                status=pl.lit(msg_status),
                status_idx=pl.lit(self._supported_status.index(msg_status)).cast(pl.Int16),
                ignore_in=pl.lit(ignore_in).cast(pl.String),
                id=pl.lit(self._id)
            )
        )

        return self
    
    def filter(self, ignored: list | str):
        """
        Get message with 'ignore_in' filter applied.
        """
        if type(ignored) == list:
            return self.df.filter(
                (
                    pl.col('ignore_in').is_in(ignored).not_()
                ) | (
                    pl.col('status').eq('rejected')    
                ) | (
                    pl.col('ignore_in').is_null()
                )
            )
        elif type(ignored) == str:
            return self.df.filter(
                (
                    (pl.col('ignore_in') != ignored) |
                    (pl.col('ignore_in').is_null())
                ) | (
                    pl.col('status').eq('rejected')
                )
            )
        else:
            raise TypeError(f'"ignored" argument with type of {type(ignored)} is invalid.')

    def as_artable(self):
        """
        Return all messages as Arrow Table.
        """
        return self.df.to_arrow()
    
    @property
    def df(self):
        return pl.concat(self._messages)
