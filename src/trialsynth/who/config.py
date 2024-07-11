from ..base.config import BaseConfig


class Config(BaseConfig):
    def __init__(self):
        super(Config, self).__init__(registry='who')
